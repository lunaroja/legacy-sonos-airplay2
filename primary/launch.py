#!/usr/bin/env python3
"""Isolated primary entry point; verify exact bundled runtime before importing."""
import hashlib,json,stat,sys
from pathlib import Path

def verify(root):
    root=Path(root).resolve();path=root/'MANIFEST.json';info=path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode&0o022 or info.st_size>2**20:raise ValueError('manifest_scope')
    manifest=json.loads(path.read_bytes())
    if manifest.get('version')!=1 or not isinstance(manifest.get('files'),dict):raise ValueError('manifest_shape')
    for name,digest in manifest['files'].items():
        relative=Path(name)
        if relative.is_absolute() or '..' in relative.parts:raise ValueError('manifest_path')
        path=root/relative;info=path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_mode&0o022 or info.st_size>128*2**20 or not path.resolve().is_relative_to(root):raise ValueError('bundle_scope')
        if hashlib.sha256(path.read_bytes()).hexdigest()!=digest:raise ValueError('bundle_digest')
    if not {p.name for p in root.glob('*.py')}<=set(manifest['files']):raise ValueError('unlisted_python')
    if not {'office_primary.py','session.py','controller.py','wav_http.py'}<=set(manifest['files']):raise ValueError('required_runtime')

def main():
    root=Path(__file__).resolve().parent
    try:verify(root)
    except Exception:print('{"status":"bundle_verification_failed"}');return 2
    sys.path.insert(0,str(root))
    from office_primary import main as entry
    return entry()
if __name__=='__main__':raise SystemExit(main())
