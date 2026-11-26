"""Primary session, configuration, WAV framing and service cleanup checks."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch,MagicMock
import json,os,socket,struct,sys,tempfile,threading,time,unittest,uuid
from configuration import load,receiver_config
from framed_pcm import Records,HEADER,Record,Fault
from session import Observer,EVENT
from session_channel import SessionChannel
from wav_http import Wave,Ring,wav_header,CAPACITY,RATE
from office_primary import Runtime,Drain
from test_primary import G
from runtime_state import StateError

class SessionTests(unittest.TestCase):
    def setUp(self):
        self.now=100.;self.o=Observer(clock=lambda:self.now);self.serial=0;self.seq=0;self.epoch=0;self.kind=0;self.pts=self.frames=self.rtp=0
        self.event(0,1,0);self.event(1,2,11);self.record(3);self.event(1,3,15);self.record(1);self.o.lineage_checked()
    def event(self,gen,event,flags,volume=0,revision=0):
        self.serial+=1
        wire=EVENT.pack(b'SPSE',2,event,flags,self.serial,gen,int(self.now*1e9),self.epoch,self.epoch and 1,self.seq,
            self.kind,self.pts,self.frames,self.rtp,uuid.UUID(G).bytes if gen else bytes(16),volume,revision)
        self.assertTrue(self.o.feed(wire))
    def record(self,kind):
        self.seq+=1;self.kind=kind
        if kind==3:self.epoch+=1
        self.pts=int((self.now+2)*1e9) if kind==1 else 0;self.frames=352 if kind in (1,2) else 0;self.rtp=100 if self.frames else 0
        self.assertTrue(self.o.observe_record(Record(kind,self.epoch,self.seq,self.pts,int(self.now*1e9),self.frames,self.rtp,bytes(self.frames*4))))
        self.event(1,4,15 if kind!=4 else 14)
    def test_fresh_media_and_quiet_hold_never_redate_observations(self):
        self.assertTrue(self.o.evidence().media);old=self.o.current.observed
        self.now+=30;self.o.lineage_checked();self.assertEqual(self.o.evidence().state,'present');self.assertFalse(self.o.evidence().media)
        self.assertEqual(self.o.current.observed,old)
    def test_fifo_stop_is_not_explicit_rtsp_end(self):
        self.record(4);self.assertEqual(self.o.evidence().state,'unproven');self.assertFalse(self.o.evidence().media)
        self.event(1,5,14);self.assertEqual(self.o.evidence().state,'ended')
    def test_volume_revisions_are_bound_and_cannot_move_back(self):
        self.event(1,7,15,-15000,1);self.assertEqual(self.o.evidence().volume_milli,-15000)
        self.serial+=1;wire=EVENT.pack(b'SPSE',2,7,15,self.serial,1,int(self.now*1e9),self.epoch,1,self.seq,self.kind,self.pts,self.frames,self.rtp,uuid.UUID(G).bytes,0,0)
        self.assertFalse(self.o.feed(wire));self.assertEqual(self.o.evidence().state,'lost')
    def test_framing_partial_bounds_and_control_payload(self):
        p=Records();body=HEADER.pack(b'SPTS',2,1,4,1,1,100,100,2,42)+b'12345678'
        self.assertEqual(p.feed(body[:10],1),[]);self.assertEqual(p.feed(body[10:],2)[0].pcm,b'12345678')
        p.feed(body[:1],2)
        with self.assertRaises(Fault):p.check(500000003)
        with self.assertRaises(Fault):Records().feed(bytes(100001),0)

class ConfigurationTests(unittest.TestCase):
    def fixture(self,value):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup);p=Path(tmp.name)/'config.json';p.write_text(json.dumps(value));p.chmod(0o600);return p
    def test_primary_examples_and_receiver_config_have_no_experiment_fields(self):
        for name in ('solo','group'):
            value=json.loads(Path(f'config.{name}.example.json').read_text());v=load(self.fixture(value));text=receiver_config(v,'/private/synthetic.fifo')
            self.assertIn('output_rate = 44100',text);self.assertIn('ignore_volume_control = "yes"',text)
            self.assertNotIn('timestamped_reanchor',text)
            self.assertEqual(v['coordinator'] is None,name=='solo')
            for word in ('multicast','silence','native_sender','borrow'):self.assertNotIn(word,text)
    def test_experimental_or_unknown_config_fields_rejected(self):
        base=json.loads(Path('config.solo.example.json').read_text())
        for name in ('multicast','silence_port','borrow','native_sender'):
            with self.assertRaises(StateError):load(self.fixture({**base,name:'unsupported'}))
    def test_stop_orders_target_cleanup_before_receiver_and_releases_every_resource(self):
        r=Runtime({});calls=[]
        r.controller=SimpleNamespace(shutdown=lambda:calls.append('target_cleanup') or True)
        for name in ('wave','drain','receiver','peers','scope'):
            setattr(r,name,SimpleNamespace(close=lambda name=name:calls.append(name)))
        self.assertTrue(r.close());self.assertEqual(calls,['target_cleanup','wave','drain','receiver','peers','scope'])
    def test_ambiguous_service_exit_keeps_recovery_state_and_still_closes_local_resources(self):
        r=Runtime({});r.controller=SimpleNamespace(shutdown=lambda:False);r.wave=MagicMock();r.receiver=MagicMock();r.scope=MagicMock()
        self.assertFalse(r.close());r.wave.close.assert_called_once();r.receiver.close.assert_called_once();r.scope.close.assert_called_once()

    def test_cleanup_exception_does_not_leak_receiver_or_scope(self):
        r=Runtime({});r.controller=SimpleNamespace(shutdown=MagicMock(side_effect=OSError('synthetic')))
        r.receiver=MagicMock();r.channel=MagicMock();r.scope=MagicMock()
        self.assertFalse(r.close());r.receiver.close.assert_called_once();r.channel.close.assert_called_once();r.scope.close.assert_called_once()

class WaveCore(unittest.TestCase):
    def test_header_and_ring_are_exact_stereo_pcm_and_bounded(self):
        values=struct.unpack('<4sI4s4sIHHIIHH4sI',wav_header())
        self.assertEqual(values,(b'RIFF',0xffffffff,b'WAVE',b'fmt ',16,1,2,44100,176400,4,16,b'data',0xffffffff))
        r=Ring()
        for _ in range(30):r.write(b'abcd'*8192)
        self.assertLessEqual(len(r.data),CAPACITY);position,audio=r.read_from(0,0);self.assertEqual(audio,b'abcd'*8192)
        r.clear();position,audio=r.read_from(position,0);self.assertEqual(audio,b'')
    def test_generation_rotation_revokes_capability_and_old_pcm(self):
        w=Wave('127.0.0.1',19091,'127.0.0.1');self.addCleanup(w.close)
        first=w.activate('one');old=w.path;w.push('one',b'abcd');second=w.activate('two')
        self.assertNotEqual(first,second);self.assertFalse(w.admit('127.0.0.1',old));w.push('one',b'abcd')
        self.assertFalse(w.ring.data);self.assertFalse(w.admit('127.0.0.2',w.path));w.revoke();self.assertFalse(w.admit('127.0.0.1',old))

@unittest.skipUnless(sys.platform.startswith('linux'),'Linux loopback socket verification')
class WaveSocket(unittest.TestCase):
    def setUp(self):
        self.present=True;self.wave=Wave('127.0.0.1',0,'127.0.0.1',present=lambda:self.present)
        self.wave.start();self.addCleanup(self.wave.close);self.wave.activate('one')
    def connect(self,path=None,method='GET',extra=''):
        s=socket.create_connection(('127.0.0.1',self.wave.port),timeout=2);self.addCleanup(s.close)
        s.sendall(f'{method} {path or self.wave.path} HTTP/1.1\r\nHost: local\r\n{extra}\r\n'.encode());return s
    def test_exact_wav_pcm_revoke_and_prompt_client_cleanup(self):
        s=self.connect();data=b''
        while b'\r\n\r\n' not in data:data+=s.recv(4096)
        header,body=data.split(b'\r\n\r\n',1);self.assertIn(b'200 OK',header)
        while len(body)<44:body+=s.recv(4096)
        self.assertEqual(body[:44],wav_header());pcm=b'\x11\x00\x12\x00'*352;self.wave.push('one',pcm)
        while len(body)<44+len(pcm):body+=s.recv(4096)
        self.assertEqual(body[44:44+len(pcm)],pcm)
        self.present=False;self.wave.revoke();self.assertEqual(s.recv(4096),b'')
        self.assertFalse(self.wave.clients)
    def test_head_range_and_bad_capability_do_not_disclose_audio(self):
        for path,method,extra,code in ((None,'HEAD','',b'200 OK'),(None,'GET','Range: bytes=0-\r\n',b'404 Not Found'),('/pcm/other.wav','GET','',b'404 Not Found')):
            with self.subTest(method=method,extra=extra):
                s=self.connect(path,method,extra);data=b''
                while True:
                    chunk=s.recv(4096)
                    if not chunk:break
                    data+=chunk
                self.assertIn(code,data);self.assertEqual(data.split(b'\r\n\r\n',1)[1],b'')
    def test_short_gap_padding_resumes_without_replaying_audio(self):
        s=self.connect();data=b''
        while len(data.split(b'\r\n\r\n')[-1])<44:data+=s.recv(4096)
        first=b'\x01\x02\x03\x04'*352;second=b'\x05\x06\x07\x08'*352
        self.wave.push('one',first);body=data.split(b'\r\n\r\n',1)[1]
        while len(body)<44+len(first):body+=s.recv(4096)
        padding=s.recv(32768);self.assertTrue(padding);self.assertEqual(set(padding),{0})
        self.wave.push('one',second);tail=b''
        while second not in tail and len(tail)<RATE*4:tail+=s.recv(32768)
        self.assertIn(second,tail);self.assertNotIn(first,tail)

if __name__=='__main__':unittest.main()

@unittest.skipUnless(sys.platform.startswith('linux'),'Linux child and framed FIFO')
class DrainIntegration(unittest.TestCase):
    def test_real_receiver_child_drains_pcm_and_explicit_end_revokes_wav(self):
        from test_session_linux import CHILD
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            fifo=Path(tmp)/'pcm';os.mkfifo(fifo,0o600);fd=os.open(fifo,os.O_RDWR|os.O_NONBLOCK)
            observer=Observer();channel=SessionChannel(observer);wave=Wave('127.0.0.1',19091,'127.0.0.1')
            options=channel.launch_options();proc=subprocess.Popen([sys.executable,'-I','-S','-B','-c',CHILD,str(fifo)],
                env=options['env_additions'],pass_fds=options['pass_fds'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            drain=None
            try:
                channel.attach(proc,fd);drain=Drain(observer,channel,fd,wave,lambda:None);drain.start()
                deadline=time.monotonic()+3
                while not observer.evidence().media and time.monotonic()<deadline:time.sleep(.01)
                self.assertTrue(observer.evidence().media);wave.activate(observer.evidence().generation)
                proc.stdin.write('timed\n');proc.stdin.flush()
                deadline=time.monotonic()+2
                while not wave.ring.data and time.monotonic()<deadline:time.sleep(.01)
                self.assertTrue(wave.ring.data)
                proc.stdin.write('end\n');proc.stdin.flush()
                deadline=time.monotonic()+2
                while wave.path and time.monotonic()<deadline:time.sleep(.01)
                self.assertEqual(observer.evidence().state,'ended');self.assertIsNone(wave.path);self.assertFalse(wave.ring.data)
                self.assertFalse(drain.error)
            finally:
                if drain:drain.close()
                proc.terminate();proc.wait(timeout=2)
                for f in (proc.stdin,proc.stdout,proc.stderr):f.close()
                channel.close();wave.close();os.close(fd)
