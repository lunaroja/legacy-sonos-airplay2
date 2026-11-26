"""Launch exactly one pinned receiver with a dedicated FIFO and private IPC."""
from pathlib import Path
import hashlib,os,secrets,stat,subprocess,time
from runtime_state import private_dir,StateError
from configuration import receiver_config
from receiver_assets import command
from receiver_diagnostics import StartupStderr

class Receiver:
    def __init__(self,configuration,root,channel):
        self.configuration=configuration;self.root=private_dir(root);self.channel=channel
        self.path=self.root/('receiver-'+secrets.token_hex(12));self.path.mkdir(mode=0o700)
        self.fifo=self.path/'pcm.fifo';os.mkfifo(self.fifo,0o600)
        self.config_path=self.path/'receiver.conf';self.proc=None;self.stderr=None
    def assert_fifo_owner(self):
        info=self.fifo.lstat()
        if not stat.S_ISFIFO(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
            raise StateError('receiver_fifo_scope')
        # This unique inode did not exist before this launcher. An unrelated
        # reader is never sought/killed; no existing service FIFO is reused.
    def start(self,fd):
        args=command(self.configuration,self.config_path)
        # SPSE v2 support is additionally checked from actual authenticated IPC.
        config=receiver_config(self.configuration,self.fifo)
        out=os.open(self.config_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(out,'w') as file:file.write(config)
        opts=self.channel.launch_options();env={'PATH':'/usr/bin:/bin','LANG':'C.UTF-8',**opts['env_additions']}
        self.proc=subprocess.Popen(args,env=env,
            pass_fds=opts['pass_fds'],close_fds=True,stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        self.stderr=StartupStderr(self.proc.stderr);self.stderr.start()
        self.channel.attach(self.proc,fd)
    def alive(self):return self.proc is not None and self.proc.poll() is None
    def diagnostics(self):
        return self.stderr.snapshot(self.proc.poll() if self.proc else None) if self.stderr else {}
    def close(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:self.proc.kill();self.proc.wait(timeout=2)
        self.channel.close()
        if self.stderr:self.stderr.close();self.stderr=None
        # Only launcher-owned paths in the unique directory are removed.
        for p in (self.config_path,self.fifo,self.fifo.with_name(self.fifo.name+'.reader.lock')):
            try:p.unlink()
            except FileNotFoundError:pass
        try:self.path.rmdir()
        except OSError:pass
