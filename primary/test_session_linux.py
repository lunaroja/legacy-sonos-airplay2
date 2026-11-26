"""Real credentialed IPC/FIFO lineage across quiet pause, END and child loss."""
from pathlib import Path
import os,select,subprocess,sys,tempfile,time,unittest
import framed_pcm as pcm
from session import Observer
from session_channel import SessionChannel

CHILD='''import os,socket,struct,sys,time,uuid
s=socket.socket(fileno=int(os.environ['SPS_SELECTION_FD']))
out=os.open(sys.argv[1],os.O_WRONLY)
E=struct.Struct('!4sBBHQQQIIIIQII16siI');H=struct.Struct('>4sBBHIIQQII')
serial=seq=epoch=first=kind=pts=frames=rtp=0
gid=uuid.UUID('12345678-1234-1234-1234-123456789abc').bytes
def event(gen,ev,flags):
 global serial
 serial+=1;s.send(E.pack(b'SPSE',2,ev,flags,serial,gen,time.monotonic_ns(),epoch,first,seq,kind,pts,frames,rtp,gid if flags & 2 else bytes(16),0,0))
def record(k):
 global seq,epoch,first,kind,pts,frames,rtp
 seq+=1;kind=k
 if k==3:epoch+=1;first=seq
 pts=time.monotonic_ns()+2000000000 if k==1 else 0
 frames=352 if k==1 else 0;rtp=100 if frames else 0
 data=H.pack(b'SPTS',2,k,4,epoch,seq,pts,time.monotonic_ns(),frames,rtp)+bytes(frames*4)
 while data:
  n=os.write(out,data);data=data[n:]
 event(1,4,15)
event(0,1,0);event(1,2,11);record(3);event(1,3,15);record(1)
print('ready',flush=True)
for command in sys.stdin:
 command=command.strip()
 if command=='flush':record(3)
 elif command=='timed':record(1)
 elif command=='end':event(1,5,14)
 elif command=='skipserial':
  serial+=1;event(1,4,15)
 elif command in ('closeipc','replaceipc','fault'):
  if command=='fault':event(1,6,16)
  fd=s.fileno();s.close()
  if command=='replaceipc':
   other=os.open('/dev/null',os.O_RDONLY)
   if other!=fd:os.dup2(other,fd);os.close(other)
 print('ready',flush=True)
'''

@unittest.skipUnless(sys.platform.startswith('linux'),'Linux /proc and SCM_CREDENTIALS required')
class KernelLineage(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name);fifo=root/'fifo';os.mkfifo(fifo,0o600)
        self.fd=os.open(fifo,os.O_RDWR|os.O_NONBLOCK);self.addCleanup(os.close,self.fd)
        self.observer=Observer();self.channel=SessionChannel(self.observer);self.addCleanup(self.channel.close)
        options=self.channel.launch_options()
        self.proc=subprocess.Popen([sys.executable,'-I','-S','-B','-c',CHILD,str(fifo)],
            env=options['env_additions'],pass_fds=options['pass_fds'],close_fds=True,
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        self.addCleanup(self.close_child);self.channel.attach(self.proc,self.fd);self.parser=pcm.Records()
        self.read_ready()
        self.assertTrue(self.observer.evidence().media)
        self.assertEqual(self.observer.evidence().state,'present')
    def close_child(self):
        if self.proc.poll() is None:self.proc.kill()
        self.proc.wait(timeout=2)
        for stream in (self.proc.stdin,self.proc.stdout,self.proc.stderr):stream.close()
    def read_ready(self):
        self.assertTrue(select.select([self.proc.stdout],[],[],2)[0],'child timeout')
        self.assertEqual(self.proc.stdout.readline(),'ready\n')
        self.channel.drain()
        while select.select([self.fd],[],[],0)[0]:
            data=os.read(self.fd,32768)
            for record in self.parser.feed(data,time.monotonic_ns()):
                self.assertTrue(self.channel.observe_record(record,self.fd))
    def command(self,value):
        self.proc.stdin.write(value+'\n');self.proc.stdin.flush();self.read_ready()
    def test_quiet_pause_uses_current_kernel_lineage_not_redated_events(self):
        observed=self.observer.current.observed;timed=self.observer.last_timed[2]
        time.sleep(2.05);self.channel.drain()
        e=self.observer.evidence()
        self.assertEqual(e.state,'present');self.assertTrue(e.state=='present')
        self.assertFalse(e.media)
        self.assertEqual(self.observer.current.observed,observed);self.assertEqual(self.observer.last_timed[2],timed)
        self.command('flush');self.assertEqual(self.observer.evidence().state,'present')
        self.assertFalse(self.observer.evidence().media)
        self.command('timed');self.assertTrue(self.observer.evidence().media)
        self.command('end');self.assertEqual(self.observer.evidence().state,'ended')
        self.assertIsNone(self.proc.poll());self.assertFalse(self.channel.closed)
    def test_process_death_invalidates_held_session(self):
        self.proc.kill();self.proc.wait(timeout=2);self.channel.drain()
        self.assertTrue(self.channel.closed);self.assertEqual(self.observer.evidence().state,'lost')
    def test_ipc_close_with_live_process_invalidates_session(self):
        self.command('closeipc');self.assertIsNone(self.proc.poll());self.assertTrue(self.channel.closed)
        self.assertEqual(self.observer.evidence().state,'lost')
    def test_live_child_with_a_serial_gap_loses_ownership_with_a_fixed_category(self):
        self.command('skipserial')
        self.assertIsNone(self.proc.poll())
        self.assertTrue(self.channel.closed)
        self.assertEqual(self.channel.failure,'event_rejected')
        self.assertEqual(self.observer.failure,'event_continuity')
        self.assertEqual(self.observer.evidence().state,'lost')

    def test_terminal_fault_is_diagnostic_only_when_inherited_fd_is_already_closed(self):
        prior_serial=self.observer.serial
        self.command('fault')
        self.assertIsNone(self.proc.poll())
        self.assertTrue(self.channel.closed)
        self.assertEqual(self.channel.failure,'receiver_reported_fault')
        self.assertEqual(self.observer.serial,prior_serial)
        self.assertEqual(self.observer.evidence().state,'lost')

    def test_inherited_fd_replaced_with_live_process_invalidates_session(self):
        self.command('replaceipc');self.assertIsNone(self.proc.poll());self.assertTrue(self.channel.closed)
        self.assertFalse(self.observer.evidence().state=='present')

if __name__=='__main__':unittest.main()
