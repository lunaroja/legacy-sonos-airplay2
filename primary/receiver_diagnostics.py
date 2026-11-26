"""Bounded receiver stderr classification. Raw bytes never leave this object."""
import os,re,select,threading,time

CATEGORIES=frozenset(('nqptp_missing','nqptp_access_denied','nqptp_access_error','nqptp_uninitialized',
 'nqptp_smi_mismatch','configuration_invalid','pipe_unavailable','observer_initialization_failed',
 'mdns_name_collision','mdns_unavailable','listen_port_unavailable','loader_failure',
 'platform_unsupported','process_permission_denied','unclassified_exit'))
PIPE_FAILURES=('pipe_invalid','pipe_poisoned','pipe_lock_timeout','pipe_write_deadline',
               'pipe_peer_closed','pipe_io_error','pipe_cancelled')

def runtime_category(line):
    # The receiver emits a fixed enum from the pinned pipe-writer patch.
    # Retain its category only, never the accompanying byte counts or text.
    match=re.search(rb'\bspts write failure code=([1-7]) bytes=[0-9]+\s*$',line.lower())
    if match:return PIPE_FAILURES[int(match[1])-1]
    if b'initialising event_sender_mutex.' in line.lower():return 'rtsp_event_init_failed'
    return None

def category(line):
    # Fixed message fragments from the pinned 5.6.3 source/observer patch. Never
    # capture or return a path, name, number, RTSP value or arbitrary text match.
    value=line.lower()
    for text,code in ((b'the nqptp service can not be found.', 'nqptp_missing'),
            (b'must have read access to the nqptp shared memory file', 'nqptp_access_denied'),
            (b'an error occurred accessing the nqptp service.', 'nqptp_access_error'),
            (b'error reading configuration file ', 'configuration_invalid'),
            (b'timestamped pipe requires ', 'configuration_invalid'),
            (b'spts reanchor requires timestamped pipe mode', 'configuration_invalid'),
            (b'could not create audio pipe ', 'pipe_unavailable'),
            (b'spts selection channel unavailable', 'observer_initialization_failed'),
            (b'could not establish a service on port ', 'listen_port_unavailable'),
            (b'error while loading shared libraries:', 'loader_failure'),
            (b'symbol lookup error:', 'loader_failure'),
            (b'error while loading ', 'loader_failure'),
            (b'shairport sync can not run on this system.', 'platform_unsupported'),
            (b'avahi: client failure:', 'mdns_unavailable'),
            (b'avahi: failed to create a client object:', 'mdns_unavailable')):
        if text in value:return code
    if b'nqptp service on this system' in value and b'not seem to be initialised' in value:return 'nqptp_uninitialized'
    if b'nqptp' in value and b'smi version' in value and b'too old' in value:return 'nqptp_smi_mismatch'
    if b'error: airplay ' in value and b' name ' in value and b' is already in use.' in value:return 'mdns_name_collision'
    if re.search(rb'line [0-9]+ of the configuration file ',value):return 'configuration_invalid'
    if b'permission denied' in value or b'operation not permitted' in value:return 'process_permission_denied'
    return None

class StartupStderr:
    WINDOW=5.0
    BUDGET=65536
    LINE=4096
    def __init__(self,stream,*,clock=time.monotonic):
        self.stream,self.clock=stream,clock;self.started=clock()
        self.lock=threading.Lock();self.stop=threading.Event();self.thread=None
        self.code=None;self.runtime_code=None;self.seen=False;self.limited=False;self.used=0
        self.buffer=bytearray();self.discard_line=False
    def _classify_line(self,startup):
        if self.discard_line:return
        line=bytes(self.buffer)
        if startup and self.code is None:self.code=category(line)
        recognized=runtime_category(line)
        if recognized is not None:self.runtime_code=recognized
    def feed(self,data):
        with self.lock:
            if data:self.seen=True
            startup=(self.clock()-self.started<=self.WINDOW and self.used<self.BUDGET
                     and self.used+len(data)<=self.BUDGET)
            if self.clock()-self.started<=self.WINDOW:self.used=min(self.BUDGET,self.used+len(data))
            if not startup and data:self.limited=True
            for byte in data:
                if byte==10:
                    self._classify_line(startup)
                    self.buffer.clear();self.discard_line=False
                elif not self.discard_line:
                    if len(self.buffer)<self.LINE:self.buffer.append(byte)
                    else:self.buffer.clear();self.discard_line=True;self.limited=True
    def finish(self):
        with self.lock:
            if self.buffer:self._classify_line(self.clock()-self.started<=self.WINDOW and self.used<=self.BUDGET)
            self.buffer.clear()
    def start(self):
        os.set_blocking(self.stream.fileno(),False)
        self.thread=threading.Thread(target=self._run,name='receiver-startup-diagnostics',daemon=True)
        self.thread.start()
    def _run(self):
        try:
            while not self.stop.is_set():
                if not select.select([self.stream],[],[],.05)[0]:self.feed(b'');continue
                try:data=os.read(self.stream.fileno(),4096)
                except BlockingIOError:continue
                if not data:break
                self.feed(data)
        except (OSError,ValueError):pass
        finally:self.finish()
    def snapshot(self,exit_status):
        if exit_status is not None and self.thread:self.thread.join(.05)
        with self.lock:
            code=self.code if self.code in CATEGORIES else None
            status=exit_status if type(exit_status)is int and -128<=exit_status<=255 else None
            if status is not None and code is None and self.runtime_code is None:code='unclassified_exit'
            return {'receiver_exit_status':status,'receiver_startup_category':code,
                    'receiver_runtime_category':self.runtime_code if status is not None else None,
                    'receiver_stderr_seen':self.seen,'receiver_stderr_limited':self.limited}
    def close(self):
        if self.thread:self.thread.join(.1)
        self.stop.set()
        if self.thread:self.thread.join(.1)
        self.stream.close();self.finish()
