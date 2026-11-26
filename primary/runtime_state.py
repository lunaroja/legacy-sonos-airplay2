"""Private durable state and a process-held product scope; no Codex dependency."""
from __future__ import annotations
from contextlib import suppress
from pathlib import Path
import fcntl,hashlib,json,os,secrets,stat,time

class StateError(RuntimeError):pass

def private_dir(path):
    p=Path(path)
    if not p.is_absolute():raise StateError('absolute_private_directory_required')
    p.mkdir(mode=0o700,parents=True,exist_ok=True)
    info=p.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
        raise StateError('private_directory_owner_or_mode')
    return p

def read_json(path,limit=2**20):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(fd,'rb') as f:
        info=os.fstat(f.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
            raise StateError('private_file_owner_or_mode')
        data=f.read(limit+1)
    if len(data)>limit:raise StateError('state_size')
    try:return json.loads(data)
    except (ValueError,UnicodeError):raise StateError('state_format') from None

def atomic_json(path,value):
    path=Path(path);data=(json.dumps(value,sort_keys=True,separators=(',',':'))+'\n').encode()
    if len(data)>2**20:raise StateError('state_size')
    temporary=path.with_name('.'+path.name+'.'+secrets.token_hex(12))
    fd=os.open(temporary,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    try:
        with os.fdopen(fd,'wb') as f:f.write(data);f.flush();os.fsync(f.fileno())
        os.replace(temporary,path)
        d=os.open(path.parent,os.O_RDONLY)
        try:os.fsync(d)
        finally:os.close(d)
    finally:
        with suppress(FileNotFoundError):temporary.unlink()

class ProductScope:
    """Cooperating local product instances only; not a lock against Sonos/apps."""
    def __init__(self,directory,peers):
        self.root=private_dir(directory);self.peers=frozenset(peers);self.files=[]
        self.identity=secrets.token_urlsafe(24);self.closed=False
        try:
            for peer in sorted(self.peers):
                path=self.root/('peer-'+hashlib.sha256(peer.encode()).hexdigest()+'.lock')
                fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
                info=os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
                    os.close(fd);raise StateError('scope_file_owner_or_mode')
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except OSError:os.close(fd);raise StateError('another_product_instance_owns_peer') from None
                self.files.append((path,fd,info.st_dev,info.st_ino))
        except BaseException:self.close();raise
    def assert_owned(self):
        if self.closed or len(self.files)!=len(self.peers):raise StateError('product_scope_lost')
        for path,fd,device,inode in self.files:
            current=path.lstat();held=os.fstat(fd)
            if (current.st_dev,current.st_ino,held.st_dev,held.st_ino)!=(device,inode,device,inode):
                raise StateError('product_scope_path_replaced')
    def close(self):
        self.closed=True
        for _,fd,_,_ in self.files:os.close(fd)
        self.files=[] # Never unlink an advisory lock inode.

class Store:
    def __init__(self,directory,config_digest):
        self.root=private_dir(directory);self.path=self.root/'active.json';self.audit=self.root/'operations.jsonl'
        self.config_digest=config_digest
        self.data=read_json(self.path) if self.path.exists() else {'version':1,'config':config_digest,'active':None,'status':'idle'}
        if self.data.get('version')!=1 or self.data.get('config')!=config_digest:
            raise StateError('state_configuration_mismatch')
        self.persist()
    def persist(self):atomic_json(self.path,self.data)
    def event(self,event,**data):
        row={'event':event,'time':time.time(),**data};wire=(json.dumps(row,sort_keys=True,separators=(',',':'))+'\n').encode()
        if len(wire)>65536:raise StateError('journal_row_bound')
        fd=os.open(self.audit,os.O_CREAT|os.O_APPEND|os.O_WRONLY|os.O_NOFOLLOW,0o600)
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:raise StateError('journal_scope')
            if info.st_size+len(wire)>8*2**20:raise StateError('journal_full_recovery_required')
            view=memoryview(wire)
            while view:
                count=os.write(fd,view)
                if count<=0:raise StateError('journal_write')
                view=view[count:]
            os.fsync(fd)
        finally:os.close(fd)
    def clean(self):
        self.data['active']=None;self.data['status']='idle';self.persist()
        # Active state is durable first. Rotate only a complete, idle journal.
        if self.audit.exists() and self.audit.stat().st_size>1024*1024:
            previous=self.root/'previous-operations.jsonl';os.replace(self.audit,previous)
    def public(self):
        active=self.data.get('active') or {}
        return {'status':self.data.get('status','unknown'),'requires_reconciliation_on_start':bool(active),
                'mode':active.get('mode'),'last_action':active.get('action'),
                'target_only':True}
