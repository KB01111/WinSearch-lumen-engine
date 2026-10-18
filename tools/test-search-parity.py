"""Actual daemon/public CLI DTO parity, optionally including offline-ranker snapshots.

Default: native document/PDF/code extraction without a model. --real-model
imports the pinned artifacts through ModelManager, requires real 768-dimensional
code vectors, and checks semantic code/image/audio/video on generated fixtures.
Both modes use an isolated data directory and a kill-on-close Windows job.
The snapshot option compares query code on the same native indexed vectors;
fixture seeding and native extraction remain separately classified evidence.
"""
import argparse
import csv
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('indexed_media', ROOT/'tools/test-indexed-media.py')
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def pdf(path):
    """Two generated text pages, using only the PDF standard and local bytes."""
    objects = [b'<< /Type /Catalog /Pages 2 0 R >>',
        b'<< /Type /Pages /Count 2 /Kids [3 0 R 4 0 R] >>',
        b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 600 800] /Resources << /Font << /F1 5 0 R >> >> /Contents 6 0 R >>',
        b'<< /Type /Page /Parent 2 0 R /MediaBox [0 0 600 800] /Resources << /Font << /F1 5 0 R >> >> /Contents 7 0 R >>',
        b'<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>']
    for text in ['First generated page.', 'pdfparitytoken The railway timetable lists trains and departure platforms.']:
        content = f'BT /F1 14 Tf 40 740 Td ({text}) Tj ET'.encode('ascii')
        objects.append(b'<< /Length '+str(len(content)).encode()+b' >>\nstream\n'+content+b'\nendstream')
    data = bytearray(b'%PDF-1.4\n')
    offsets = [0]
    for number, obj in enumerate(objects, 1):
        offsets.append(len(data))
        data.extend(f'{number} 0 obj\n'.encode()+obj+b'\nendobj\n')
    xref = len(data)
    data.extend(f'xref\n0 {len(offsets)}\n0000000000 65535 f \n'.encode())
    for offset in offsets[1:]:
        data.extend(f'{offset:010d} 00000 n \n'.encode())
    data.extend(f'trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n'.encode())
    path.write_bytes(data)


class Frames(harness.Pipe):
    def send(self, message):
        raw = json.dumps(message).encode('utf-8')
        frame = memoryview(struct.pack('<I', len(raw))+raw)
        while frame:
            count = self.stream.write(frame)
            assert count, 'pipe write failed'
            frame = frame[count:]

    def read(self):
        length = struct.unpack('<I', self.exact(4))[0]
        assert 0 < length <= 16*1024*1024
        return json.loads(self.exact(length))

    def begin(self, query):
        self.sequence += 1
        self.send({'type': 'search', 'id': self.sequence, 'query': query})
        return self.sequence

    def next_result(self, request):
        while True:
            reply = self.read()
            if reply.get('id') == request:
                assert reply['type'] == 'results', reply
                return reply

    def search_frames(self, query):
        request = self.begin(query)
        frames = []
        while not frames or not frames[-1]['done']:
            frames.append(self.next_result(request))
        return frames


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--real-model', action='store_true')
    parser.add_argument('--compare-offline', action='store_true',
        help='also compare shared offline query functions against native DB snapshots')
    parser.add_argument('--compare-client', action='store_true',
        help='exercise ws-client against the same owned native engine and source fixtures')
    options = parser.parse_args()
    output = options.output.resolve()
    assert output.is_relative_to(ROOT/'artifacts') and not output.exists()
    output.mkdir(parents=True)
    binary = ROOT/'src/engine/target/debug'
    model = ROOT/'artifacts/test-models/embeddinggemma-2-Q8_0.gguf'
    projector = model.with_name('mmproj-embeddinggemma-2-Q8_0.gguf')
    summary = {'scope': __doc__, 'real_model_requested': options.real_model,
        'queries': [], 'checks': [], 'failures': [], 'quality_misses': [], 'owned_job_empty': False,
        'harness_sha256': digest(Path(__file__)),
        'expected_pipeline_version': 2, 'expected_semantic_floor': .65,
        'expected_code_query_policy': 'code-and-search-task-corroboration-v1',
        'binary_sha256': {name: digest(binary/name) for name in
            ['winsearchd.exe', 'winsearch-cli.exe', 'winsearch-embed.exe', 'winsearch-extract.exe']},
        'adjacent_native_dll_sha256': {p.name: digest(p) for p in sorted(binary.glob('*.dll'))}}
    if options.real_model:
        summary['model_sha256'] = digest(model)
        summary['projector_sha256'] = digest(projector)
    if options.compare_offline:
        summary['offline_comparator_sha256'] = digest(binary/'examples/native-eval-parity.exe')
    if options.compare_client:
        summary['headless_client_sha256'] = digest(binary/'examples/native-client-smoke.exe')
    (output/'input-lock.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    sid = next(csv.reader(subprocess.check_output(['whoami', '/user', '/fo', 'csv', '/nh'], text=True).splitlines()))[1]
    instance = 'parity-'+uuid.uuid4().hex
    pipe_name = r'\\.\pipe\winsearch-'+sid+'-'+instance
    owner = harness.OwnedJob()
    child = control = client = None
    timer = threading.Timer(300, owner.terminate)
    timer.daemon = True
    start = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix='winsearch-search-parity-') as directory:
        generated = Path(directory)
        documents = generated/'documents'
        code = generated/'code'
        documents.mkdir(); code.mkdir()
        (documents/'item-a.txt').write_text('Generated notes.\namberparitytoken Chocolate cake uses flour butter eggs and sugar.\n', encoding='utf-8')
        pdf(documents/'item-b.pdf')
        source = code/'源 code.rs'
        source.write_text('pub fn parityneedle() {}\nfn caller() { parityneedle(); }\n', encoding='utf-8')
        (code/'cells.ipynb').write_text(json.dumps({'metadata': {'language_info': {'name': 'python'}}, 'cells': [
            {'cell_type': 'markdown', 'source': ['Generated notebook']},
            {'cell_type': 'code', 'source': ['def notebookparity():\n', '    return 1\n'], 'metadata': {},
                'outputs': [{'text':['PRIVATE_OUTPUT_SENTINEL'],'output_type':'stream','name':'stdout'}]}]}), encoding='utf-8')
        if options.real_model:
            for original, name in [('test-models/screen.png', 'item-c.png'),
                    ('spoken-fixtures/two-passages-65s.wav', 'item-d.wav'),
                    ('media-fixtures/時刻 clip.mp4', 'item-e.mp4')]:
                shutil.copyfile(ROOT/'artifacts'/original, documents/name)
        summary['generated_source_sha256'] = {str(p.relative_to(generated)): digest(p) for p in generated.rglob('*') if p.is_file()}
        (output/'input-lock.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
        environment = {**os.environ, 'WINSEARCH_DATA_DIR': str(output/'data'),
            'WINSEARCH_INSTANCE_ID': instance, 'WS_PIPE': pipe_name,
            'WS_EMBED_STUB': '0', 'WS_EMBED_BACKEND': 'cpu',
            'WS_EMBED_MODEL': str(output/'data/models'/model.name if options.real_model else generated/'missing.gguf'),
            'WINSEARCH_TEST_DECODER_DIR': str(ROOT/'artifacts/dependencies/ffmpeg-2026-09-30/ffmpeg-n9.0.2-17-g2a571b6068-win64-lgpl-shared-9.0/bin')}

        def owned_command(args, timeout=45):
            # Join the job before this fixed waiting wrapper launches the CLI.
            wrapper = 'import subprocess,sys; sys.stdin.buffer.read(1); sys.exit(subprocess.call(sys.argv[1:]))'
            process = subprocess.Popen([sys.executable, '-c', wrapper, *args], env=environment,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NO_WINDOW)
            owner.assign(process)
            stdout, stderr = process.communicate(input=b'1', timeout=timeout)
            assert process.returncode == 0, stderr.decode('utf-8', errors='replace')
            return stdout, stderr

        def cli(text, mode, semantic):
            args = [str(binary/'winsearch-cli.exe'), 'search', text, '--json', '--mode', mode, '--limit', '5']
            if semantic: args.append('--semantic')
            stdout, _ = owned_command(args)
            return json.loads(stdout)

        def compare_offline():
            if not options.compare_offline:return
            snapshots={}
            for name in ['index','code']:
                destination=output/f'{name}-snapshot.db'
                with sqlite3.connect((output/f'data/db/{name}.db').as_uri()+'?mode=ro',uri=True) as source:
                    with sqlite3.connect(destination) as target:source.backup(target)
                snapshots[name]=destination
            manifest={'index':str(snapshots['index']),'code_index':str(snapshots['code']),
                'model':str(model) if options.real_model else None,
                'cases':[{'name':case['name'],'query':case['query'],'expected':case['cli']}
                    for case in summary['queries'] if case['name']!='code_disabled']}
            input_path=output/'offline-input.json'
            input_path.write_text(json.dumps(manifest,indent=2),encoding='utf-8')
            summary['offline_input_sha256']=digest(input_path)
            summary['offline_snapshot_sha256']={name:digest(path) for name,path in snapshots.items()}
            result_path=output/'offline-results.json'
            args=[str(binary/'examples/native-eval-parity.exe'),str(input_path),str(result_path)]
            stdout,stderr=owned_command(args,timeout=90)
            (output/'offline-worker.log').write_bytes(stdout+stderr)
            summary['offline_parity']=json.loads(result_path.read_text(encoding='utf-8'))
            assert summary['offline_parity']['equal'],summary['offline_parity']
            summary['checks'].append('shared_offline_ranker_native_snapshot_ordered_ids_and_locations')

        def compare(name, text, mode='files', semantic=False, expected=None, location=None, empty=False,
                    expected_rank=1, excerpt=True):
            query = {'text': text, 'mode': mode, 'limit': 5, 'include_semantic': semantic}
            frames = client.search_frames(query)
            hits = [hit for frame in frames for hit in frame['batch']]
            actual = cli(text, mode, semantic)
            record = {'name': name, 'query': query, 'frames': frames, 'cli': actual,'expect_empty':empty}
            summary['queries'].append(record)
            assert actual == hits, ('DTO parity', name, hits, actual)
            assert len({hit['id'] for hit in hits}) == len(hits), ('duplicate IDs', name)
            if empty: assert not hits, (name, hits)
            if expected:
                record['expected_name'] = expected
                record['expected_max_rank'] = expected_rank
                record['modality']=('code' if mode=='code' else {'.png':'image','.wav':'audio','.mp4':'video'}.get(Path(expected).suffix,'document'))
                rank=next((index+1 for index,hit in enumerate(hits[:5]) if Path(hit['path']).name==expected),None)
                record['relevant_rank']=rank
                if not semantic:assert rank is not None and rank<=expected_rank,(name,hits)
                if rank is None:
                    summary['quality_misses'].append(name)
                else:
                    assert rank<=expected_rank,(name,hits)
                    relevant=hits[rank-1]
                    if location: assert relevant['location'] == location, (name, hits)
                    if excerpt: assert relevant.get('excerpt'), (name, 'missing source excerpt', hits)
            return hits

        def wait_for(condition, timeout=180):
            deadline = time.perf_counter()+timeout
            while not condition():
                assert child.poll() is None and time.perf_counter() < deadline, 'index did not become ready'
                time.sleep(.2)

        try:
            with (output/'daemon.log').open('wb') as log:
                child = subprocess.Popen([str(binary/'winsearchd.exe'), '--test-stdin-gate'],
                    env=environment, stdin=subprocess.PIPE, stdout=log, stderr=log,
                    creationflags=subprocess.CREATE_NO_WINDOW)
            owner.assign(child)
            timer.start()
            child.stdin.write(b'\1'); child.stdin.close()
            control = harness.Pipe(pipe_name, child)
            client = Frames(pipe_name, child)
            compare('code_disabled', 'parityneedle', mode='code', empty=True)
            if options.real_model:
                # Exercise the production model manager as well as the worker;
                # a worker-only model override leaves code vectors unavailable.
                imported = control.request('model_import', path=str(model.parent))
                assert imported['type'] == 'model_status', imported
                def model_ready():
                    status = control.request('get_model_status')['status']
                    assert status['state'] != 'error', status
                    summary['model_import_status'] = status
                    return status['state'] == 'ready'
                wait_for(model_ready)
                for model_source in [model, projector]:
                    staged = output/'data/models'/model_source.name
                    assert digest(staged) == digest(model_source), ('staged model bytes changed', model_source.name)
                summary['checks'].append('pinned_model_import_ready_and_staged_artifact_hashes')
            control.request('set_config', config={'semantic_enabled': options.real_model, 'preferred_backend': 'cpu'})
            config = control.request('get_indexing_config')['config']
            config.update(roots=[str(documents), str(code)], code_enabled=True, code_roots=[str(code)],
                respect_battery_saver=False, index_on_battery=True)
            control.request('set_indexing_config', config=config)
            db = output/'data/db/index.db'
            def ready():
                with sqlite3.connect(db.as_uri()+'?mode=ro', uri=True) as connection:
                    progress = connection.execute('SELECT complete FROM content_progress').fetchall()
                    pending = connection.execute("SELECT count(*) FROM content_chunks WHERE embed=1 AND embed_state!='done'").fetchone()[0]
                    # Filename embedding can remain pending with semantic search
                    # disabled; exact extraction must still become usable.
                    jobs = connection.execute("SELECT state FROM extract_jobs WHERE kind!='name'").fetchall()
                assert all(row[0]!='failed' for row in jobs),('native extraction failed',jobs)
                enough = len(progress) >= (5 if options.real_model else 2)
                complete = enough and all(row[0] for row in progress) and all(row[0] == 'done' for row in jobs)
                return complete and (not options.real_model or pending == 0)
            wait_for(ready)
            wait_for(lambda: bool(control.request('search', query={'text': 'parityneedle', 'mode': 'code', 'limit': 5})))
            if options.real_model:
                def code_vectors_ready():
                    with sqlite3.connect((output/'data/db/code.db').as_uri()+'?mode=ro', uri=True) as connection:
                        total, embedded, width = connection.execute(
                            'SELECT count(*),sum(norm>0),sum(norm>0 AND length(vector)=1536) FROM code_chunks').fetchone()
                    summary['code_vectors'] = {'chunks':total, 'embedded':embedded or 0, 'width_768':width or 0}
                    return total > 0 and total == embedded == width
                wait_for(code_vectors_ready)
                summary['checks'].append('native_code_index_has_complete_768d_vectors')
            summary['index_seconds'] = round(time.perf_counter()-start, 3)
            with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as connection:
                ordinary=connection.execute('SELECT e.id,e.path,p.revision,p.source_sha256,p.source_sha256_revision FROM entries e JOIN content_progress p ON p.entry_id=e.id ORDER BY e.path').fetchall()
            assert len(ordinary)==(5 if options.real_model else 2),ordinary
            summary['ordinary_provenance']=[]
            for entry_id,path,revision,actual_hash,hash_revision in ordinary:
                assert revision==hash_revision and revision>0 and actual_hash==digest(Path(path)),('ordinary byte provenance mismatch',path,actual_hash,hash_revision)
                resolved=control.request('resolve_entry',reference={'result_id':f'e{entry_id}','source_revision':None})
                assert resolved['type']=='resolved_entry' and resolved['entry']['content_sha256']==actual_hash,resolved
                summary['ordinary_provenance'].append({'path':path,'revision':revision,'hash_revision':hash_revision,'sha256':actual_hash,'resolved':resolved['entry']})
            summary['checks'].append('native_ordinary_document_media_byte_sha256_and_resolved_metadata')
            document_hits = compare('document_exact', 'amberparitytoken', expected='item-a.txt', location={'kind': 'line', 'line': 2})
            document_hit = next(hit for hit in document_hits if Path(hit['path']).name == 'item-a.txt')
            reference = {'result_id':document_hit['id'],'source_revision':None}
            resolved = control.request('resolve_entry', reference=reference)
            assert resolved['type']=='resolved_entry',resolved
            entry = resolved['entry']
            assert entry['result_id']==document_hit['id'] and Path(entry['path'])==documents/'item-a.txt',entry
            assert Path(entry['root_path'])==documents and entry['size_bytes']==(documents/'item-a.txt').stat().st_size,entry
            assert entry['index_revision']>0 and entry['source_revision'],entry
            reference['source_revision']=entry['source_revision']
            assert control.request('resolve_entry',reference=reference)['entry']==entry
            summary['entry_resolution']={'admitted':resolved}
            summary['checks'].append('native_indexed_entry_resolution_with_current_source_proof')
            compare('pdf_exact', 'pdfparitytoken', expected='item-b.pdf', location={'kind': 'page', 'page': 2})
            hits = compare('code_definition_reference', 'parityneedle', mode='code', expected=source.name, location={'kind': 'line', 'line': 1})
            assert len(hits) == 2 and hits[1]['location'] == {'kind': 'line', 'line': 2}, hits
            assert [hit['id'].split(':',4)[3] for hit in hits]==['true','false'],hits
            compare('notebook', 'notebookparity', mode='code', expected='cells.ipynb',
                location={'kind': 'notebook', 'cell_ordinal': 1, 'line': 1, 'end_line': 2})
            with sqlite3.connect((output/'data/db/code.db').as_uri()+'?mode=ro', uri=True) as connection:
                provenance = connection.execute('SELECT f.path,f.content_revision,s.sha256 FROM code_files f JOIN code_source_state s ON s.file_id=f.id ORDER BY f.path').fetchall()
                assert connection.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]=='4'
            assert len(provenance)==2,provenance
            for path, revision, actual_hash in provenance:
                assert revision > 0 and actual_hash == digest(Path(path)),('code byte provenance mismatch',path,revision,actual_hash)
            summary['code_provenance']=[{'path':path,'revision':revision,'sha256':actual_hash} for path,revision,actual_hash in provenance]
            summary['checks'].append('native_code_and_notebook_exact_byte_sha256_provenance')
            compare('code_isolation', 'parityneedle', empty=True)
            compare('absent_exact', 'unfindableparitytoken', empty=True)
            compare('code_absent_exact', 'unfindableparitytoken', mode='code', empty=True)
            summary['checks'].append('native_extraction_exact_code_opt_in_and_payload_parity')
            if options.compare_client:
                stdout, stderr = owned_command([str(binary/'examples/native-client-smoke.exe')], timeout=90)
                (output/'headless-client.log').write_bytes(stdout+stderr)
                native_client = json.loads(stdout)
                # The client intentionally changes policy; bound proofs must change with that policy.
                def ranking(hits):
                    return [{key:({k:v for k,v in value.items() if k!='source_revision'} if key=='entry_reference' and value else value)
                        for key,value in hit.items()} for hit in hits]
                assert ranking(native_client['document']) == ranking(document_hits), ('client document ranking parity', native_client)
                expected_code = next(record['cli'] for record in summary['queries']
                    if record['name'] == 'code_definition_reference')
                assert ranking(native_client['code']) == ranking(expected_code), ('client code ranking parity', native_client)
                assert len(native_client['reference_checks'])==4,native_client
                for entry in [*native_client['code_entries'],native_client['document_preview']['entry'],native_client['pdf_preview']['entry']]:
                    assert entry['content_sha256']==digest(Path(entry['path'])),entry
                summary['checks'].append('typed_client_bound_code_notebook_refs_and_actual_sha256')
                assert len(native_client['semantic_checks']) == (3 if options.real_model else 0), native_client
                assert len(native_client['preview_checks']) == 4, native_client
                assert len(native_client['code_preview_checks']) == 4,native_client
                summary['headless_client'] = native_client
                summary['checks'].append('typed_async_client_native_dto_status_pause_revoke_restore_reconnect')
            if not options.real_model:
                with sqlite3.connect((output/'data/db/code.db').as_uri()+'?mode=ro', uri=True) as connection:
                    assert connection.execute('SELECT count(*) FROM code_chunks WHERE norm>0').fetchone()[0] == 0
                summary['checks'].append('native_code_exact_with_zero_model_vectors')
                compare_offline()
            if options.real_model:
                worker = harness.Pipe(r'\\.\pipe\winsearch-embed-'+sid+'-'+instance, child)
                status = worker.request('status'); worker.stream.close()
                assert status['model_loaded'] and status['stub'] is False and status['backend'] == 'cpu' and status['dim'] == 768, status
                summary['worker'] = status
                compare('document_semantic', 'baking a chocolate cake with flour butter eggs and sugar', semantic=True, expected='item-a.txt', expected_rank=5)
                compare('pdf_semantic', 'railway timetable for trains and departure platforms', semantic=True, expected='item-b.pdf', expected_rank=5)
                compare('semantic_negative', 'asteroid orbital mechanics and interstellar propulsion', semantic=True, empty=True)
                compare('code_semantic_stream', 'parityneedle', mode='code', semantic=True, expected=source.name, location={'kind': 'line', 'line': 1})
                code_query = 'a function named parityneedle and its caller'
                compare('code_semantic_only_exact_control', code_query, mode='code', empty=True)
                code_hits = compare('code_semantic_only', code_query, mode='code', semantic=True,
                    expected=source.name, expected_rank=5)
                source_lines = source.read_text(encoding='utf-8').splitlines()
                returned_lines = set()
                for hit in code_hits:
                    if Path(hit['path']) != source: continue
                    match = hit['location']
                    assert match['kind'] == 'line' and 1 <= match['line'] <= len(source_lines), hit
                    assert hit['excerpt'].rstrip('\r\n') == source_lines[match['line']-1], hit
                    returned_lines.add(match['line'])
                assert returned_lines == {1,2}, ('semantic query requested both function and caller', code_hits)
                if options.compare_client:
                    assert ranking(summary['headless_client']['semantic_code']) == ranking(code_hits), 'typed semantic-only code ranking/reference identity parity'
                compare('code_semantic_negative', 'asteroid orbital mechanics and interstellar propulsion',
                    mode='code', semantic=True, empty=True)
                summary['checks'].append('native_semantic_only_code_function_and_caller_source_lines')
                for name, text, expected in [
                    ('image_semantic', 'a computer application screenshot with a red warning and a blue button', 'item-c.png'),
                    ('audio_semantic', 'the spoken explanation of baking chocolate cake in the kitchen', 'item-d.wav'),
                    ('video_semantic', 'a screenshot in a video with a red warning and a blue button', 'item-e.mp4')]:
                    hits = compare(name, text, semantic=True, expected=expected, expected_rank=5,
                        excerpt=False)
                    relevant = next((hit for hit in hits if Path(hit['path']).name == expected),None)
                    if relevant is None:continue
                    if name in ['audio_semantic', 'video_semantic']:
                        assert relevant['location']['kind'] == 'timestamp', relevant
                        lower,upper=(50,65) if name=='audio_semantic' else (32.75,65)
                        begin,end=(relevant['location'][key] for key in ['seconds','end_seconds'])
                        assert all(isinstance(value,(int,float)) and not isinstance(value,bool)
                            and math.isfinite(value) for value in [begin,end]),relevant
                        assert lower<=begin<=end<=upper,relevant
                    if name == 'audio_semantic':
                        assert not relevant.get('excerpt'), 'audio embeddings must not invent a transcript'
                compare_offline()
                # A cold query first yields the exact-stage frame. Cancel while
                # real CPU semantic inference remains outstanding, then require
                # an empty terminal frame and a live public transport.
                request = client.begin({'text': 'a railway journey timetable with scheduled train departure platforms',
                    'mode': 'files', 'limit': 5, 'include_semantic': True})
                first = client.next_result(request)
                assert not first['done'], first
                client.send({'type': 'cancel', 'id': request})
                terminal = client.next_result(request)
                assert terminal['done'] and not terminal['batch'], terminal
                assert client.request('ping')['type'] == 'pong'
                summary['cancel_frames'] = [first, terminal]
                summary['checks'].append('real_in_flight_cancel_empty_terminal_transport_live')
                request = client.begin({'text': 'a schedule showing railway trains and their departure platform information',
                    'mode': 'files', 'limit': 5, 'include_semantic': True})
                first = client.next_result(request)
                assert not first['done'], first
                config.update(roots=[], code_roots=[])
                control.request('set_indexing_config', config=config)
                terminal = client.next_result(request)
                assert terminal['done'] and not terminal['batch'], terminal
                summary['revocation_frames'] = [first, terminal]
                summary['checks'].append('real_in_flight_root_revocation_empty_terminal')
            config.update(roots=[], code_roots=[])
            control.request('set_indexing_config', config=config)
            client.sequence+=1
            client.send({'type':'resolve_entry','id':client.sequence,'reference':reference})
            while True:
                denied=client.read()
                if denied.get('id')==client.sequence:break
            assert denied['type']=='error' and denied['code']=='not_ready' and 'entry' not in denied,denied
            summary['entry_resolution']['revoked']=denied
            summary['checks'].append('native_entry_reference_rejected_after_root_revocation')
            compare('revoked_document', 'amberparitytoken', empty=True)
            compare('revoked_code', 'parityneedle', mode='code', empty=True)
            if options.real_model:
                compare('revoked_semantic', 'baking a chocolate cake with flour butter eggs and sugar', semantic=True, empty=True)
            summary['checks'].append('live_root_revocation_public_cli_and_pipe')
            positive=[record for record in summary['queries'] if 'expected_name' in record]
            recall=sum(record['relevant_rank'] is not None for record in positive)/len(positive)
            mrr=sum(1/record['relevant_rank'] if record['relevant_rank'] else 0 for record in positive)/len(positive)
            # Use the existing overall quality gates, retaining every individual
            # miss; DTO/location errors and negative results still fail directly.
            summary['quality']={'positive_queries':len(positive),'Recall@5':recall,'MRR':mrr,
                'minimum_recall_at_5':.90,'minimum_mrr':.85,'maximum_negative_fp_rate':0}
            original=[record for record in positive if record['name']!='code_semantic_only']
            original_recall=sum(record['relevant_rank'] is not None for record in original)/len(original)
            original_mrr=sum(1/record['relevant_rank'] if record['relevant_rank'] else 0 for record in original)/len(original)
            summary['quality']['original_positive_cases']={'positive_queries':len(original),
                'Recall@5':original_recall,'MRR':original_mrr}
            assert original_recall>=.90 and original_mrr>=.85,summary['quality']
            empty_controls=[record for record in summary['queries'] if record['expect_empty']]
            summary['quality'].update(empty_result_controls=len(empty_controls),
                empty_control_fp_rate=sum(bool(record['cli']) for record in empty_controls)/len(empty_controls),
                empty_control_scope='all declared empty-result cases, including code isolation and root revocation')
            summary['quality']['by_modality']={}
            for modality in sorted({record['modality'] for record in positive}):
                group=[record for record in positive if record['modality']==modality]
                summary['quality']['by_modality'][modality]={'positive_queries':len(group),
                    'Recall@5':sum(record['relevant_rank'] is not None for record in group)/len(group),
                    'MRR':sum(1/record['relevant_rank'] if record['relevant_rank'] else 0 for record in group)/len(group)}
            assert recall>=.90 and mrr>=.85,summary['quality']
            summary['checks'].append('unchanged_overall_recall_mrr_and_zero_negative_controls')
        except BaseException as error:
            summary['failures'].append(repr(error))
            raise
        finally:
            timer.cancel()
            for connection in [client, control]:
                if connection: connection.stream.close()
            owner.terminate()
            if child: child.wait(timeout=10)
            deadline = time.perf_counter()+5
            while owner.sample() and time.perf_counter() < deadline: time.sleep(.05)
            summary['owned_job_empty'] = not owner.sample()
            summary['pids'] = sorted(owner.pids_seen)
            owner.close()
            summary['elapsed_seconds'] = round(time.perf_counter()-start, 3)
            (output/'results.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
            assert summary['owned_job_empty'], 'owned descendants survived'
    print(json.dumps({'output': str(output), 'queries': len(summary['queries']), 'checks': summary['checks'],
        'failures': summary['failures'], 'owned_job_empty': summary['owned_job_empty'], 'elapsed_seconds': summary['elapsed_seconds']}, indent=2))


if __name__ == '__main__':
    main()
