"""Verify copied-package CPU/NPU inference and recoverable component failures."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def module(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools' / file)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


payload = module('payload', 'assemble-engine-payload.py')
runtime = module('media_runtime', 'test-media-runtime.py')
transport = module('owned_media', 'test-indexed-media.py')


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def clean_environment(data):
    result = {key: value for key, value in os.environ.items()
              if not key.startswith(('WS_', 'WINSEARCH_', 'LLAMA_', 'XRT_', 'XILINX_', 'TACONITE_', 'GGML_', 'VK_', 'VULKAN_', 'CUDA_'))}
    result.update(PATH=str(Path(os.environ['SystemRoot']) / 'System32'),
                  WINSEARCH_DATA_DIR=str(data), WINSEARCH_INSTANCE_ID=uuid.uuid4().hex,
                  RUST_LOG='ws_embed=info,winsearch_embed=info')
    return result


def fallback_case(worker, model, bundle, working, output, reason, label):
    pipe = r'\\.\pipe\winsearch-package-recovery-' + uuid.uuid4().hex
    owner = transport.OwnedJob()
    child = None
    client = None
    result = {'case': label, 'expectedNpuReason': reason, 'workerSha256': sha(worker)}
    with (output / ('fallback-' + label + '.log')).open('wb') as log:
        try:
            child = subprocess.Popen([str(worker), '--pipe', pipe, '--model', str(model),
                '--backend', 'npu', '--npu-bundle', str(bundle)], cwd=working,
                env=clean_environment(working / ('data-' + uuid.uuid4().hex)),
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, creationflags=subprocess.CREATE_NO_WINDOW)
            owner.assign(child)
            timer = threading.Timer(120, owner.terminate)
            timer.daemon = True
            timer.start()
            try:
                client = transport.Pipe(pipe, child)
                for _ in range(2):
                    reply = client.request('embed_texts', texts=['task: search result | query: a train to Stockholm'])
                    runtime.validate_vectors(reply, 1, 'cpu')
                    assert reply.get('npu') == reason, reply
                status = client.request('status')
                assert status['model_loaded'] is True and status['backend'] == 'cpu' and status['npu'] == reason
                assert client.request('shutdown')['type'] == 'shutdown_ack'
                client.stream.close()
                client = None
                child.wait(timeout=5)
                assert child.returncode == 0
                result.update(backend='cpu', stub=False, npu=reason, passed=True)
            finally:
                timer.cancel()
        finally:
            if client:
                client.stream.close()
            if child and child.poll() is None:
                owner.terminate()
                child.wait(timeout=5)
            deadline = time.monotonic() + 5
            while owner.sample() and time.monotonic() < deadline:
                time.sleep(.025)
            result['ownedJobEmpty'] = not owner.sample()
            owner.close()
    assert result['ownedJobEmpty']
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['payload', 'models', 'fixtures', 'spoken', 'consumer-root', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--manifest-sha256', required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    source = payload.no_links(args.payload)
    assert sha(source / 'payload.json') == args.manifest_sha256
    manifest = json.loads((source / 'payload.json').read_text())
    payload.require_commit(manifest['engineCommit'])
    files = {item['path']: item for item in manifest['files']}
    assert {p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()} == {*files, 'payload.json'}
    result = {'engineCommit': manifest['engineCommit'], 'manifestSha256': args.manifest_sha256,
              'outsideBothCheckouts': True, 'developerOverridesRemoved': True,
              'modelFixtureStorage': 'separate external copies, outside the engine package and both checkouts',
              'fallback': [], 'passed': False}
    with tempfile.TemporaryDirectory(prefix='winsearch-npu-package-') as directory:
        working = Path(directory).resolve()
        assert not working.is_relative_to(ROOT.resolve())
        assert not working.is_relative_to(args.consumer_root.resolve())
        deployed = working / 'engine'
        deployed.mkdir()
        for name, item in files.items():
            payload.checked_copy(source / name, payload.archive_destination(deployed, name), item['sha256'])
        shutil.copyfile(source / 'payload.json', deployed / 'payload.json')
        weights = working / 'weights'
        weights.mkdir()
        for name in ['embeddinggemma-2-Q8_0.gguf', 'mmproj-embeddinggemma-2-Q8_0.gguf']:
            model = payload.no_links(args.models / name)
            shutil.copyfile(model, weights / name)
        pins = (ROOT / 'src/engine/crates/ws-modelmgr/src/manifest.rs').read_text()
        expected = re.findall(r'name:\s*(TEXT_FILE|MMPROJ_FILE),\s*size:\s*([0-9_]+),\s*sha256:\s*Some\("([0-9a-f]{64})"\)', pins)
        assert len(expected) == 2
        for kind, size, digest in expected:
            name = 'embeddinggemma-2-Q8_0.gguf' if kind == 'TEXT_FILE' else 'mmproj-embeddinggemma-2-Q8_0.gguf'
            assert (weights / name).stat().st_size == int(size.replace('_', '')) and sha(weights / name) == digest
        bundle = weights / 'iron-embeddinggemma2-npu2'
        original_manifest = payload.no_links(args.models / bundle.name / 'manifest.txt')
        original_manifest_hash = sha(original_manifest)
        for model in (args.models / bundle.name).rglob('*'):
            if model.is_file():
                payload.no_links(model)
                target = bundle / model.relative_to(args.models / bundle.name)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(model, target)
        fixtures = working / 'fixtures'
        fixtures.mkdir()
        for name in ['reference-media-vectors.json', 'photo.jpg', 'screen.png', 'speech.wav']:
            shutil.copyfile(args.fixtures / name, fixtures / name)
        spoken = working / 'spoken'
        spoken.mkdir()
        for name in ['railway.wav', 'kitchen.wav']:
            shutil.copyfile(args.spoken / name, spoken / name)
        worker = deployed / 'winsearch-embed.exe'
        for backend in ['npu', 'cpu']:
            target = args.output.resolve() / backend
            subprocess.run([sys.executable, str(ROOT / 'tools/test-media-runtime.py'),
                '--worker', str(worker), '--model', str(weights / 'embeddinggemma-2-Q8_0.gguf'),
                '--fixtures', str(fixtures), '--spoken', str(spoken), '--output', str(target),
                '--backend', backend, '--npu-bundle', str(bundle), '--warm-repetitions', '5',
                '--clean-env', '--working-directory', str(working)], check=True, cwd=working)
            data = json.loads((target / 'results.json').read_text())
            assert data['workerSha256'] == files['winsearch-embed.exe']['sha256'] and data['ownedJobEmpty']
            result[backend] = data
        # Fault variants affect only the disposable clone; the pinned package
        # remains immutable. A missing shim must still allow CPU inference.
        (deployed / 'iron_xrt_shim.dll').unlink()
        model = weights / 'embeddinggemma-2-Q8_0.gguf'
        result['fallback'].append(fallback_case(worker, model, bundle, working, args.output, 'load_failed', 'missing-shim'))
        result['fallback'].append(fallback_case(worker, model, working / 'missing-bundle', working, args.output, 'bundle_missing', 'missing-bundle'))
        payload.checked_copy(source / 'iron_xrt_shim.dll', deployed / 'iron_xrt_shim.dll', files['iron_xrt_shim.dll']['sha256'])
        # Corrupt only the external copy; the source model remains unchanged.
        (bundle / 'manifest.txt').unlink()
        (bundle / 'manifest.txt').write_bytes(b'corrupt bundle fixture')
        result['fallback'].append(fallback_case(worker, model, bundle, working, args.output, 'bundle_missing', 'corrupt-bundle'))
        assert sha(original_manifest) == original_manifest_hash
        assert all(sha(source / name) == item['sha256'] for name, item in files.items())
        result['passed'] = True
    (args.output / 'results.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'passed': result['passed'], 'manifestSha256': args.manifest_sha256,
                      'recoveryCases': len(result['fallback'])}, indent=2))


if __name__ == '__main__':
    main()
