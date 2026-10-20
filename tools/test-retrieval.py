"""Run the actual CLI under an owned Windows job, preserving failures and hashes."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time
import uuid

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('suite', choices=['names', 'retrieval-development', 'retrieval-held-out',
                                    'code-development', 'code-held-out'])
parser.add_argument('output')
parser.add_argument('--binary-dir', type=Path, default=root/'src/engine/target/debug',
    help='directory containing the coherent CLI, worker and native DLLs')
args = parser.parse_args()
output = (root/args.output).resolve()
assert output.is_relative_to(root/'artifacts') and not output.exists()
output.mkdir(parents=True)
spec = importlib.util.spec_from_file_location('indexed_media_harness', root/'tools/test-indexed-media.py')
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)
owner = harness.OwnedJob()
model = root/'artifacts/test-models/embeddinggemma-2-Q8_0.gguf'
projector = root/'artifacts/test-models/mmproj-embeddinggemma-2-Q8_0.gguf'
binary = args.binary_dir.resolve()/'winsearch-cli.exe'
worker = args.binary_dir.resolve()/'winsearch-embed.exe'
suite = root/'evals'/f'{args.suite}.json'
frozen = {
    'names': '4640467f2867f24e61c2c5d6184ee1bb8200b1938807f36798fded8c55050037',
    'retrieval-development': 'd8830a7fe8ed0491b8752634bce741b9d909afceebf2ec0c8a2de7be3450bbe9',
    'retrieval-held-out': '4fba3a5dee353621501bba66a9ae785ce55e4d0f1a016abe450831545e2a58bd',
    'code-development': '34a0a9951173d7f9e8469d66e36054269ca5a697541d6bb3d83742ab9d033f93',
    'code-held-out': '38daace215c27241f6321b1a7f4e51bc38c2baaf5c8cb1719fdf5c40ec0fed86',
}
def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()
assert digest(suite) == frozen[args.suite], 'Frozen fixture changed'
assets = sorted({segment['asset'] for entry in json.loads(suite.read_text(encoding='utf-8'))['entries']
    for segment in entry.get('segments', []) if segment.get('asset')})
asset_hashes = {}
for asset in assets:
    resolved = (root/asset).resolve()
    assert resolved.is_relative_to(root/'artifacts/retrieval-fixtures'), 'Unexpected generated asset path'
    asset_hashes[asset] = digest(resolved)
lock = {
    'suite': args.suite, 'suite_sha256': digest(suite),
    'cli_sha256': digest(binary), 'model_sha256': digest(model),
    'projector_sha256': digest(projector), 'backend_requested': 'cpu',
    'worker_sha256': digest(worker), 'generated_asset_sha256': asset_hashes,
    'recipe_frozen_commit': '4173543b8e434d9bef912623935cdc8b53aeeee6',
    'expected_pipeline_version': 2, 'expected_minimum_semantic_cosine': .65,
    'expected_code_query_policy': 'code-and-search-task-corroboration-v1',
    'input_scope': 'declared fixture chunks and actual generated media, not native extraction proof',
    'workspace_files_at_invocation_not_build_manifest': {name: digest(root/name) for name in [
        'src/engine/crates/ws-modelmgr/src/recipe.rs',
        'src/engine/crates/ws-index/src/retrieval.rs',
        'src/engine/bins/winsearch-cli/src/eval.rs',
        'src/engine/crates/ws-code/src/store.rs',
    ]},
}
(output/'evaluation-lock.json').write_text(json.dumps(lock, indent=2), encoding='utf-8')
environment = {**os.environ, 'WS_EMBED_TEST_MODEL': str(model), 'WS_EMBED_STUB': '0',
    'WS_EMBED_BACKEND': 'cpu', 'WINSEARCH_DATA_DIR': str(output/'data'),
    'WINSEARCH_INSTANCE_ID': 'eval-'+uuid.uuid4().hex,
    'WS_EVAL_EXE': str(binary), 'WS_EVAL_SUITE': str(suite)}
# The fixed wrapper waits for input. It joins the owned job BEFORE it launches
# CLI/worker descendants, so there is no assign-after-spawn escape window.
script = r'''
$OutputEncoding = [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$null = [Console]::ReadLine()
& $env:WS_EVAL_EXE eval $env:WS_EVAL_SUITE
exit $LASTEXITCODE
'''
child = None
timeout = None
started = time.perf_counter()
summary = {**lock, 'exit_code': None, 'timed_out': False}
try:
    with (output/'eval.log').open('wb') as log:
        child = subprocess.Popen(['powershell', '-NoProfile', '-NonInteractive', '-Command', script],
            cwd=root, env=environment, stdin=subprocess.PIPE, stdout=log, stderr=log,
            creationflags=subprocess.CREATE_NO_WINDOW)
        owner.assign(child)
        def stop():
            summary['timed_out'] = True
            owner.terminate()
        timeout = threading.Timer(240, stop)
        timeout.start()
        child.stdin.write(b'owned\n')
        child.stdin.close()
        while child.poll() is None:
            owner.sample()
            time.sleep(.05)
        summary['exit_code'] = child.returncode
finally:
    if timeout:
        timeout.cancel()
        timeout.join(timeout=2)
    owner.terminate()
    if child:
        child.wait(timeout=5)
    deadline = time.perf_counter()+5
    while owner.sample() and time.perf_counter()<deadline:
        time.sleep(.025)
    summary['owned_job_empty'] = not owner.sample()
    owner.close()
    summary['elapsed_seconds'] = round(time.perf_counter()-started, 3)
    summary['pids'] = sorted(owner.pids_seen)
    log = (output/'eval.log').read_text(encoding='utf-8', errors='replace')
    for metric in ['Recall@5', 'MRR', 'Negative FP rate']:
        found = re.search(r'^'+re.escape(metric)+r':\s+([0-9.]+)', log, re.M)
        summary[metric] = float(found.group(1)) if found else None
    summary['failures'] = [line.strip() for line in log.splitlines()
        if line.strip().startswith(('[MISS]', '[FAIL (FP)]'))]
    summary['real_worker_verified'] = 'real cpu; stub:false; dim:768;' in log
    actual_pipeline = re.search(r'^vector pipeline:\s+(\d+)', log, re.M)
    actual_floor = re.search(r'^semantic floor:\s+([0-9.]+)', log, re.M)
    summary['actual_pipeline_version'] = int(actual_pipeline.group(1)) if actual_pipeline else None
    summary['actual_minimum_semantic_cosine'] = float(actual_floor.group(1)) if actual_floor else None
    summary['passed'] = summary['exit_code']==0 and 'Result:            PASS' in log
    (output/'results.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps({k: summary[k] for k in ['suite','exit_code','timed_out','owned_job_empty',
        'elapsed_seconds','Recall@5','MRR','Negative FP rate','failures','real_worker_verified','passed']}, indent=2))
assert summary['owned_job_empty']
assert not summary['timed_out']
assert summary['real_worker_verified'], 'No verified real-model report'
assert summary['actual_pipeline_version']==lock['expected_pipeline_version'], 'Wrong recipe pipeline'
assert summary['actual_minimum_semantic_cosine']==lock['expected_minimum_semantic_cosine'], 'Wrong admission floor'
assert summary['passed'], 'Evaluation did not report a passing result'
raise SystemExit(summary['exit_code'] if summary['exit_code'] is not None else 1)
