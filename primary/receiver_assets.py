"""Pinned executable/loader/library assets; no shell wrapper or environment hooks."""
from pathlib import Path
import hashlib,json,os,re,stat
from runtime_state import StateError

LIMIT=128*2**20

def pinned_file(path,digest,*,executable=False,limit=LIMIT):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(fd,'rb') as file:
        info=os.fstat(file.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_mode&0o022 or info.st_uid not in (0,os.getuid())
                or info.st_size>limit or executable and not info.st_mode&0o111):raise StateError('receiver_asset_scope')
        h=hashlib.sha256()
        for chunk in iter(lambda:file.read(1048576),b''):h.update(chunk)
        if h.hexdigest()!=digest:raise StateError('receiver_asset_digest')

def command(configuration,config_path):
    c=configuration['receiver'];pinned_file(c['executable'],c['sha256'],executable=True)
    args=[c['executable'],'-c',str(config_path)]
    loader=c.get('loader')
    if not loader:return args
    pinned_file(loader['manifest'],loader['manifest_sha256'],limit=1048576)
    manifest=json.loads(Path(loader['manifest']).read_bytes())
    files=manifest.get('files')
    if (manifest.get('version')!=1 or not isinstance(files,dict) or not 1<=len(files)<=256
            or files.get('ld-linux-aarch64.so.1')!=loader['sha256']):raise StateError('receiver_library_manifest')
    directory=Path(loader['library_dir']);info=directory.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_mode&0o022 or info.st_uid not in (0,os.getuid())
            or Path(loader['executable'])!=directory/'ld-linux-aarch64.so.1'
            or {p.name for p in directory.iterdir()}!=set(files)):raise StateError('receiver_library_directory')
    for name,digest in files.items():
        if not isinstance(name,str) or not re.fullmatch(r'[A-Za-z0-9_.+-]{1,128}',name) or not isinstance(digest,str) or not re.fullmatch(r'[0-9a-f]{64}',digest):
            raise StateError('receiver_library_entry')
        pinned_file(directory/name,digest,executable=name=='ld-linux-aarch64.so.1')
    # Popen starts this loader as the receiver PID; loading the ELF image does not
    # fork. The only inherited control fd remains SPS_SELECTION_FD. No LD_* env.
    return [loader['executable'],'--inhibit-cache','--library-path',str(directory),*args]
