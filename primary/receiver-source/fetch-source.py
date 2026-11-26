#!/usr/bin/env python3
"""Fetch one public source archive and verify its content pin before use."""
import hashlib
import json
import os
from pathlib import Path
import urllib.request

root = Path(__file__).resolve().parent
pin = json.loads((root / 'source.json').read_text())
cache = root / '.cache'
cache.mkdir(exist_ok=True)
destination = cache / pin['archive']
def valid(path):
    return path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == pin['sha256']
if not valid(destination):
    temporary = cache / (pin['archive'] + '.partial')
    try:
        with urllib.request.urlopen(pin['url'], timeout=45) as response, temporary.open('wb') as out:
            size = 0
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                if size > 16 * 1024 * 1024:
                    raise ValueError('source archive exceeds size bound')
                out.write(chunk)
        if not valid(temporary):
            raise ValueError('source archive checksum mismatch')
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
print(pin['sha256'] + '  ' + pin['archive'])
