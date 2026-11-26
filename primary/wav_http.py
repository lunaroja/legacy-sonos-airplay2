"""Bounded WAV relay using the released standalone rate, gap and reconnect behavior."""
import hmac,secrets,socket,struct,threading,time
from socketserver import BaseRequestHandler,ThreadingTCPServer
RATE=44100
CAPACITY=RATE*4*3
GAP_SECONDS=5.0

def wav_header():
    return struct.pack('<4sI4s4sIHHIIHH4sI',b'RIFF',0xffffffff,b'WAVE',b'fmt ',16,1,2,RATE,RATE*4,4,16,b'data',0xffffffff)

class Ring:
    def __init__(self):self.condition=threading.Condition();self.data=bytearray();self.end=0
    def write(self,data):
        if not isinstance(data,bytes) or len(data)%4 or len(data)>32768:raise ValueError('pcm_chunk_bound')
        with self.condition:
            self.data.extend(data);self.end+=len(data)
            if len(self.data)>CAPACITY:del self.data[:-CAPACITY]
            self.condition.notify_all()
    def clear(self):
        with self.condition:self.data.clear();self.condition.notify_all()
    def join_pos(self):
        with self.condition:return max(self.end-len(self.data),self.end-RATE*4//10)
    def read_from(self,position,timeout=.25):
        with self.condition:
            if position>=self.end:self.condition.wait(timeout)
            position=max(position,self.end-len(self.data));count=min(32768,max(0,self.end-position))
            offset=position-(self.end-len(self.data));return position+count,bytes(self.data[offset:offset+count])

class Wave:
    def __init__(self,host,port,target_ip,*,present=lambda:False):
        self.host,self.port,self.target_ip=host,port,target_ip;self.present=present
        self.ring=Ring();self.lock=threading.RLock();self.generation=None;self.path=None
        self.clients=set();self.server=self.thread=None;self.last_audio=0
    def activate(self,generation):
        with self.lock:
            if self.generation!=generation:
                self.revoke();self.generation=generation;self.path='/pcm/'+secrets.token_urlsafe(32)+'.wav'
            return f'http://{self.host}:{self.port}{self.path}'
    def push(self,generation,data):
        with self.lock:
            if generation!=self.generation:return
            self.ring.write(data);self.last_audio=time.monotonic()
    def flush(self):self.ring.clear()
    def revoke(self):
        with self.lock:
            self.generation=self.path=None;self.ring.clear()
            for client in list(self.clients):
                try:client.shutdown(socket.SHUT_RDWR)
                except OSError:pass
                client.close()
            self.clients.clear()
    def admit(self,ip,path):
        with self.lock:return bool(self.generation and self.path and ip==self.target_ip and hmac.compare_digest(path,self.path))
    def start(self):
        owner=self
        class Handler(BaseRequestHandler):
            def handle(self):
                client=self.request;client.settimeout(2);data=bytearray();deadline=time.monotonic()+2
                try:
                    while b'\r\n\r\n' not in data:
                        remaining=deadline-time.monotonic()
                        if remaining<=0 or len(data)>8192:raise ValueError()
                        client.settimeout(remaining);chunk=client.recv(min(1024,8193-len(data)))
                        if not chunk:raise ValueError()
                        data.extend(chunk)
                    header,body=bytes(data).split(b'\r\n\r\n',1);lines=header.decode('ascii').split('\r\n')
                    method,path,version=lines[0].split(' ')
                    if method not in ('GET','HEAD') or version not in ('HTTP/1.0','HTTP/1.1') or len(lines)>33 or body:raise ValueError()
                    fields={}
                    for line in lines[1:]:
                        key,value=line.split(':',1);key=key.lower()
                        if key in fields:raise ValueError()
                        fields[key]=value.strip()
                    if any(k in fields for k in ('range','transfer-encoding')) or fields.get('content-length','0')!='0' or not owner.admit(self.client_address[0],path):raise ValueError()
                except (OSError,ValueError,UnicodeError):
                    try:client.sendall(b'HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
                    except OSError:pass
                    return
                client.settimeout(2)
                try:
                    client.sendall(b'HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n')
                    if method=='HEAD':return
                    with owner.lock:
                        if not owner.admit(self.client_address[0],path):return
                        owner.clients.add(client)
                    position=owner.ring.join_pos();last_audio=None;padding=0
                    client.sendall(wav_header())
                    while owner.admit(self.client_address[0],path):
                        position,pcm=owner.ring.read_from(position)
                        now=time.monotonic()
                        if pcm:last_audio=now;padding=0;client.sendall(pcm)
                        elif last_audio is not None and owner.present():
                            due=int(min(now-last_audio,GAP_SECONDS)*RATE)*4
                            count=min(32768,max(0,due-padding));count-=count%4
                            if count:client.sendall(bytes(count));padding+=count
                        # Detect closed peers promptly even while PCM is idle.
                        try:
                            readable=__import__('select').select([client],[],[],0)[0]
                            if readable and not client.recv(1,socket.MSG_PEEK):break
                        except (OSError,ValueError):break
                except OSError:pass
                finally:
                    with owner.lock:owner.clients.discard(client)
        class Server(ThreadingTCPServer):
            allow_reuse_address=True;daemon_threads=True;request_queue_size=4
            slots=threading.BoundedSemaphore(4)
            def process_request(self,request,address):
                if not self.slots.acquire(False):request.close();return
                try:super().process_request(request,address)
                except BaseException:self.slots.release();raise
            def process_request_thread(self,request,address):
                try:super().process_request_thread(request,address)
                finally:self.slots.release()
        self.server=Server((self.host,self.port),Handler);self.port=self.server.server_address[1]
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.1},daemon=True);self.thread.start()
    def close(self):
        self.revoke()
        if self.server:self.server.shutdown();self.server.server_close();self.server=None
        if self.thread:self.thread.join(2);self.thread=None
