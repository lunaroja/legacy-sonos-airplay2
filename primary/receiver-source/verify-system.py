#!/usr/bin/env python3
"""Check a reviewed receiver ELF using host libraries; never start a receiver."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('binary', type=Path)
p.add_argument('--sha256', required=True)
a = p.parse_args()
if platform.system() != 'Linux' or platform.machine() != 'aarch64':
    p.error('Linux ARM64 required')
if not re.fullmatch('[0-9a-f]{64}', a.sha256):
    p.error('expected SHA256 must be 64 lowercase hex digits')
binary = a.binary.resolve(strict=True)
if hashlib.sha256(binary.read_bytes()).hexdigest() != a.sha256:
    p.error('receiver checksum mismatch; refusing execution')
loader = Path('/lib/ld-linux-aarch64.so.1')
environment = {'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C'}
listing = subprocess.run([str(loader), '--list', str(binary)], capture_output=True, text=True,
                         timeout=10, env=environment)
if listing.returncode or 'not found' in listing.stdout + listing.stderr:
    print(json.dumps({'system_dependencies_resolved': False, 'receiver_started': False}))
    raise SystemExit(2)
paths = {}
for line in listing.stdout.splitlines():
    match = re.match(r'\s*(\S+) => (/\S+) ', line)
    if match:
        name, path = match.groups()
        resolved = Path(path).resolve()
        if not str(resolved).startswith(('/usr/lib/', '/lib/')):
            raise ValueError('dependency resolved outside system library directories')
        paths[name] = str(resolved)
version = subprocess.check_output([str(binary), '-V'], text=True, stderr=subprocess.STDOUT,
                                  timeout=10, env=environment).strip()
if not version.startswith('5.6.3-AirPlay2-smi11-'):
    raise ValueError('unexpected receiver version or clock interface')
print(json.dumps({'sha256': a.sha256, 'version': version, 'system_dependencies_resolved': True,
                  'libraries': paths, 'receiver_started': False, 'live_playback_verified': False},
                 indent=2, sort_keys=True))
