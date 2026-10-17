"""Generated code only: real daemon, owned parser children, public named pipe.
No model inference or editor/UI acceptance is claimed by this check.
"""
import csv
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import struct
import subprocess
import tempfile
import threading
import time
import uuid

root=Path(__file__).resolve().parent.parent
spec=importlib.util.spec_from_file_location('media_harness',root/'tools/test-indexed-media.py')
harness=importlib.util.module_from_spec(spec);spec.loader.exec_module(harness)
output=root/'artifacts'/('code-task2-pipe-'+uuid.uuid4().hex[:8]);output.mkdir()
sid=next(csv.reader(subprocess.check_output(['whoami','/user','/fo','csv','/nh'],text=True).splitlines()))[1]
summary={'scope':__doc__,'checks':[],'model_inference':False}
start=time.perf_counter()
with tempfile.TemporaryDirectory(prefix='winsearch-code-pipe-') as directory:
    generated=Path(directory);repo=generated/'repo';repo.mkdir()
    source=repo/'源 code.rs';source.write_text('pub fn needle() {}\nfn caller() { needle(); }\n',encoding='utf-8')
    (repo/'.gitignore').write_text('ignored.rs\n',encoding='utf-8')
    (repo/'ignored.rs').write_text('fn hidden() {}\n',encoding='utf-8')
    (repo/'cells.ipynb').write_text(json.dumps({'metadata':{'language_info':{'name':'python'}},'cells':[
        {'cell_type':'markdown','source':['text']},
        {'cell_type':'code','source':['def notebook_symbol():\n','    return 1\n'],'metadata':{},'outputs':[]}]}),encoding='utf-8')
    data=output/'data';instance='code-'+uuid.uuid4().hex
    owner=harness.OwnedJob();child=None;pipe=None
    timer=threading.Timer(150,owner.terminate);timer.daemon=True;timer.start()
    def launch():
        global child,pipe
        log=(output/f'daemon-{len(summary["checks"])}.log').open('wb')
        child=subprocess.Popen([str(root/'src/engine/target/debug/winsearchd.exe'),'--test-stdin-gate'],stdin=subprocess.PIPE,stdout=log,stderr=log,creationflags=subprocess.CREATE_NO_WINDOW,
            env={**os.environ,'WINSEARCH_DATA_DIR':str(data),'WINSEARCH_INSTANCE_ID':instance,'WS_EMBED_MODEL':str(generated/'missing.gguf'),'WS_EMBED_STUB':'0','WS_EMBED_BACKEND':'cpu'})
        log.close();owner.assign(child);child.stdin.write(b'\1');child.stdin.close()
        pipe=harness.Pipe(r'\\.\pipe\winsearch-'+sid+'-'+instance,child)
    def stop():
        global child,pipe
        if pipe:pipe.stream.close();pipe=None
        owner.terminate()
        if child:child.wait(timeout=10)
        deadline=time.perf_counter()+5
        while owner.sample() and time.perf_counter()<deadline:time.sleep(.01)
        assert not owner.sample(),'owned descendants survived termination'
    def search(text,mode='code',limit=5):
        return pipe.request('search',query={'text':text,'mode':mode,'limit':limit,'include_semantic':False})
    def wait_result(text,expected,timeout=15):
        deadline=time.perf_counter()+timeout
        while True:
            hits=search(text)
            if bool(hits)==expected:return hits
            assert time.perf_counter()<deadline,(text,hits)
            time.sleep(.05)
    try:
        launch()
        assert not search('needle');summary['checks'].append('disabled_code_empty')
        config=pipe.request('get_indexing_config')['config']
        config.update(roots=[],code_enabled=True,code_roots=[str(repo)],paused=True,respect_battery_saver=False,index_on_battery=True)
        pipe.request('set_indexing_config',config=config)
        time.sleep(.15);assert not search('needle');summary['checks'].append('paused_initial_parse_deferred')
        config['paused']=False;pipe.request('set_indexing_config',config=config)
        hits=wait_result('needle',True)
        assert len(hits)>=2 and hits[0]['location']['line']==1 and hits[1]['location']['line']==2,hits
        assert hits[0]['path']==str(source) and hits[0]['excerpt']
        assert hits[0]['id']!=hits[1]['id'];summary['checks'].append('native_exact_definition_before_reference_no_model')
        assert not any(h['kind']=='code' for h in search('needle','auto'));assert not search('hidden')
        notebook=wait_result('notebook_symbol',True)[0]
        assert notebook['location']=={'kind':'notebook','cell_ordinal':1,'line':1,'end_line':2},notebook
        summary['checks'].extend(['ordinary_query_separation','gitignore','notebook_actual_cell_local_line'])
        config['paused']=True;pipe.request('set_indexing_config',config=config)
        assert search('needle');summary['checks'].append('pause_preserves_existing_exact')
        stamp=source.stat();source.write_text('pub fn changed() {}\nfn caller() { changed(); }\n',encoding='utf-8')
        os.utime(source,ns=(stamp.st_atime_ns,stamp.st_mtime_ns))
        wait_result('needle',False)
        changed_at=time.perf_counter()
        config['paused']=False;pipe.request('set_indexing_config',config=config)
        assert not search('changed'),'resume bypassed quiet period'
        stop();summary['checks'].append('owned_restart_with_pending_quiet_work')
        launch();assert not search('changed'),'restart bypassed durable quiet period'
        assert not search('needle'),'stale text reappeared on restart'
        while time.perf_counter()-changed_at<58:
            assert not search('changed'),'production quiet period was shortened'
            time.sleep(.5)
        wait_result('changed',True,timeout=12)
        summary['quiet_seconds']=round(time.perf_counter()-changed_at,3)
        summary['checks'].append('production_60_second_quiet_persisted_through_resume_restart')
        config['code_roots']=[];pipe.request('set_indexing_config',config=config)
        assert not search('changed');summary['checks'].append('root_revocation_immediately_hides')
        # Cancel is in-band and followed by a live ping; no-model searches have
        # already completed. Deterministic in-flight cancellation is tested by Rust.
        raw=json.dumps({'type':'cancel','id':999999}).encode();pipe.stream.write(struct.pack('<I',len(raw))+raw)
        assert pipe.request('ping')['type']=='pong';summary['checks'].append('in_band_cancel_transport_remains_live')
        with sqlite3.connect((data/'db/index.db').as_uri()+'?mode=ro',uri=True) as db:
            assert not db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'code_%'").fetchall()
        with sqlite3.connect((data/'db/code.db').as_uri()+'?mode=ro',uri=True) as db:
            assert db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]=='3'
            assert db.execute('SELECT count(*) FROM code_chunks WHERE norm>0').fetchone()[0]==0
        summary['checks'].append('separate_code_db_schema3_no_model_vectors')
    finally:
        stop();timer.cancel();owner.close()
        summary['owned_job_empty']=True;summary['elapsed_seconds']=round(time.perf_counter()-start,3)
        (output/'results.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
print(json.dumps({'output':str(output),**summary},indent=2))
