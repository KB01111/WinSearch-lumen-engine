"""Frozen canonical fixture bytes, independent of Git's Windows EOL conversion."""
import hashlib

FROZEN = {
    'names': 'd9cd4cf74454bd00ae57de7130afbbdbbc6aa5c1788c9fc37da9a4bf9682af15',
    'retrieval-development': 'd8830a7fe8ed0491b8752634bce741b9d909afceebf2ec0c8a2de7be3450bbe9',
    'retrieval-held-out': '4fba3a5dee353621501bba66a9ae785ce55e4d0f1a016abe450831545e2a58bd',
    'code-development': '34a0a9951173d7f9e8469d66e36054269ca5a697541d6bb3d83742ab9d033f93',
    'code-held-out': '38daace215c27241f6321b1a7f4e51bc38c2baaf5c8cb1719fdf5c40ec0fed86',
}


def verify_fixture(path, suite):
    raw = path.read_bytes()
    canonical = hashlib.sha256(raw.replace(b'\r\n', b'\n')).hexdigest()
    if canonical != FROZEN[suite]:
        raise ValueError('Frozen fixture changed')
    return canonical
