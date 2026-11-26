#!/usr/bin/env python3
"""Build the content-pinned receiver inside the pinned offline ARM64 toolchain."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tarfile

RECIPE = Path('/recipe')
OUTPUT = Path('/out')
BUILD = Path('/build')

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def run(args, log, cwd, env):
    with (OUTPUT / log).open('w') as stream:
        subprocess.run(args, cwd=cwd, env=env, check=True, stdout=stream,
                       stderr=subprocess.STDOUT, timeout=300)

def main():
    if platform.machine() != 'aarch64':
        raise RuntimeError('ARM64 toolchain required')
    if any(OUTPUT.iterdir()):
        raise RuntimeError('output directory must be empty')
    pin = json.loads((RECIPE/'source.json').read_text())
    archive = RECIPE/'.cache'/pin['archive']
    inputs = {'.cache/'+pin['archive']: pin['sha256']}
    inputs.update({p['path']: p['sha256'] for p in pin['patches']})
    for name, digest in inputs.items():
        if sha(RECIPE/name) != digest:
            raise ValueError('input checksum mismatch: '+name)
    source = BUILD/'shairport-sync-5.6.3'
    if source.exists():
        raise RuntimeError('fresh build directory required')
    with tarfile.open(archive) as tf:
        for member in tf.getmembers():
            path = Path(member.name)
            if (path.is_absolute() or '..' in path.parts or not path.parts or
                    path.parts[0] != source.name or not (member.isfile() or member.isdir())):
                raise ValueError('unexpected source archive entry')
        tf.extractall(BUILD, filter='data')
    env = dict(os.environ, SOURCE_DATE_EPOCH=str(pin['source_date_epoch']),
               TZ='UTC', LC_ALL='C', LANG='C')
    with (OUTPUT/'patches.log').open('w') as stream:
        for patch in pin['patches']:
            stream.write(patch['path']+'\n'); stream.flush()
            with (RECIPE/patch['path']).open('rb') as data:
                subprocess.run(['patch', '--batch', '--fuzz=0', '-p1'], cwd=source,
                               env=env, stdin=data, stdout=stream, stderr=subprocess.STDOUT,
                               timeout=20, check=True)
    run(['autoreconf', '-fi'], 'autoreconf.log', source, env)
    run(['./configure', *pin['configure']], 'configure.log', source, env)
    run(['make', '-j2'], 'make.log', source, env)
    binary = OUTPUT/'shairport-sync'
    shutil.copyfile(source/'shairport-sync', binary); binary.chmod(0o755)
    version = subprocess.check_output([str(binary), '-V'], env=env, timeout=10, text=True).strip()
    if not version.startswith('5.6.3-AirPlay2-smi11-') or '-pipe-' not in version:
        raise ValueError('unexpected receiver version/features')
    (OUTPUT/'version.txt').write_text(version+'\n')
    ldd = subprocess.check_output(['ldd', str(binary)], env=env, timeout=10, text=True)
    if 'not found' in ldd:
        raise RuntimeError('unresolved system library')
    (OUTPUT/'ldd.txt').write_text(ldd)
    elf = subprocess.check_output(['readelf', '-d', str(binary)], env=env, text=True)
    needed = re.findall(r'\(NEEDED\).*\[(.*?)\]', elf)
    (OUTPUT/'elf-dynamic.txt').write_text(elf)
    libraries = {}
    for line in ldd.splitlines():
        match = re.match(r'\s*(\S+) => (/\S+) ', line)
        if not match:
            continue
        name, location = match.groups(); path = Path(location).resolve()
        ownership = subprocess.run(['dpkg-query', '-S', str(path)], capture_output=True, text=True)
        if ownership.returncode:
            ownership = subprocess.run(['dpkg-query', '-S', location], capture_output=True, text=True, check=True)
        package = ownership.stdout.split(': ', 1)[0]
        version_line = subprocess.check_output(['dpkg-query', '-W', '-f=${Version}', package], text=True)
        libraries[name] = {'path': str(path), 'package': package, 'version': version_line,
                           'sha256': sha(path), 'direct_dependency': name in needed}
    (OUTPUT/'runtime-libraries.json').write_text(json.dumps(libraries, indent=2, sort_keys=True)+'\n')
    shutil.copytree('/recipe-metadata', OUTPUT/'toolchain')
    notices = OUTPUT/'licenses'; notices.mkdir()
    for name in ('LICENSES', 'COPYING', 'tinyhttp/LICENSE'):
        destination = notices/name; destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source/name, destination)
    # Preserve actual component license comments alongside the upstream index.
    headers = {}
    for file in sorted(source.rglob('*')):
        if file.suffix not in ('.c', '.h', '.cpp', '.hpp'):
            continue
        content = file.read_text(errors='replace')
        blocks = [b for b in re.findall(r'/\*.*?\*/', content, re.S)
                  if re.search(r'copyright', b, re.I) and
                  re.search(r'license|redistribut|permission|warranty', b, re.I)]
        if blocks:
            headers[str(file.relative_to(source))] = blocks
    (notices/'source-license-headers.json').write_text(json.dumps(headers, indent=2, sort_keys=True)+'\n')
    shutil.copyfile(RECIPE/'LICENSE', notices/'PROJECT-LICENSE')
    shutil.copyfile(RECIPE/'NOTICES.md', notices/'NOTICES.md')
    generated = OUTPUT/'generated'; generated.mkdir()
    for name in ('session_observer.c', 'session_observer.h'):
        shutil.copyfile(source/name, generated/name)
    pins = {'source': pin, 'inputs': inputs,
            'architecture': platform.machine(), 'distribution': Path('/etc/os-release').read_text(),
            'toolchain_image': os.environ.get('RECIPE_TOOLCHAIN_IMAGE'),
            'base_image': 'debian@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2',
            'debian_snapshot': '20261117T000000Z',
            'compiler': subprocess.check_output(['cc', '--version'], text=True).splitlines()[0],
            'receiver_version': version, 'sha256': sha(binary), 'needed': needed,
            'patched_source': {name: sha(source/name) for name in ('session_observer.c','session_observer.h','rtsp.c')},
            'compiled_without_network': True, 'live_playback_verified': False}
    (OUTPUT/'build.json').write_text(json.dumps(pins, indent=2, sort_keys=True)+'\n')
    (OUTPUT/'SHA256SUMS').write_text(''.join(sha(file)+'  '+str(file.relative_to(OUTPUT))+'\n'
                                          for file in sorted(OUTPUT.rglob('*')) if file.is_file()))
    print(json.dumps({'receiver_sha256': sha(binary), 'version': version,
                      'system_libraries_resolved': len(libraries), 'live_playback_verified': False}))

if __name__ == '__main__':
    main()
