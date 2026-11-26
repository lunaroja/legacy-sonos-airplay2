"""Primary-only configuration, with an optional compatible Sonos coordinator."""
from pathlib import Path
import hashlib,json,os,re,stat
from sonos_api import Endpoint
from fresh_airplay_gid import Target
from runtime_state import StateError

KEYS={'version','local_ip','target','coordinator','wav_port','event_port','state_dir','scope_dir','receiver','max_volume'}
def load(path):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(fd,'rb') as file:
        info=os.fstat(file.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size>32768 or info.st_mode&0o022 or info.st_uid not in (0,os.getuid()):raise StateError('configuration_scope')
        raw=file.read(32769)
    value=json.loads(raw)
    if not isinstance(value,dict) or set(value)!=KEYS or value['version']!=1:raise StateError('configuration_fields')
    for who in ('target','coordinator'):
        peer=value[who]
        if who=='coordinator' and peer is None:continue
        keys={'ip','uuid','port'}|({'airplay_instance'} if who=='coordinator' else set())
        if not isinstance(peer,dict) or set(peer)!=keys:raise StateError('configured_peer')
        Endpoint(peer['ip'],peer['uuid'],peer['port'])
        if who=='coordinator':Target(peer['ip'],peer['airplay_instance'])
    Endpoint(value['local_ip'],value['target']['uuid'])
    if value['coordinator'] and (value['coordinator']['uuid']==value['target']['uuid'] or value['coordinator']['ip']==value['target']['ip']):raise StateError('distinct_peers')
    for name in ('wav_port','event_port'):
        if type(value[name])is not int or not 1024<=value[name]<=65535:raise StateError('configured_port')
    # Coordinator GENA uses the next port; target events never depend on it.
    if value['event_port']==65535 or value['wav_port'] in (value['event_port'],value['event_port']+1):raise StateError('distinct_ports')
    if type(value['max_volume'])is not int or not 1<=value['max_volume']<=100:raise StateError('max_volume')
    for name in ('state_dir','scope_dir'):
        if not isinstance(value[name],str) or not Path(value[name]).is_absolute():raise StateError('absolute_runtime_directory')
    c=value['receiver'];required={'executable','sha256','name','port'}
    if not isinstance(c,dict) or not required<=set(c) or set(c)-required-{'device_id','nqptp_interface','loader'}:raise StateError('receiver_fields')
    if not isinstance(c['executable'],str) or not Path(c['executable']).is_absolute() or not re.fullmatch(r'[0-9a-f]{64}',c['sha256']):raise StateError('receiver_binary_pin')
    if not re.fullmatch(r'[A-Za-z0-9 ._-]{1,48}',c['name']):raise StateError('receiver_name')
    if type(c['port'])is not int or not 1024<=c['port']<=65535 or c['port'] in (value['wav_port'],value['event_port'],value['event_port']+1):raise StateError('receiver_port')
    c.update(receiver_identity(value))
    if 'loader' in c:
        loader=c['loader'];keys={'executable','sha256','library_dir','manifest','manifest_sha256'}
        if not isinstance(loader,dict) or set(loader)!=keys:raise StateError('loader_fields')
        for name in ('executable','library_dir','manifest'):
            if not isinstance(loader[name],str) or not Path(loader[name]).is_absolute():raise StateError('loader_path')
        for name in ('sha256','manifest_sha256'):
            if not re.fullmatch(r'[0-9a-f]{64}',loader[name]):raise StateError('loader_pin')
        if Path(loader['executable'])!=Path(loader['library_dir'])/'ld-linux-aarch64.so.1':raise StateError('loader_location')
    return value

def endpoint(v,who):
    p=v[who];return Endpoint(p['ip'],p['uuid'],p['port']) if p else None

def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def receiver_identity(v):
    c=v['receiver'];target=v['target']['uuid'];device=c.get('device_id','02'+hashlib.sha256(target.encode('ascii')).hexdigest()[:10])
    interface=c.get('nqptp_interface','/nqptp-'+target.lower())
    if not isinstance(device,str) or not re.fullmatch(r'[0-9A-Fa-f]{12}',device) or int(device[:2],16)&3!=2:raise StateError('receiver_device_id')
    if not isinstance(interface,str) or not re.fullmatch(r'/[A-Za-z0-9._-]{1,62}',interface):raise StateError('receiver_nqptp_interface')
    return {'device_id':device.lower(),'nqptp_interface':interface}

def receiver_config(v,fifo):
    c=v['receiver'];identity=receiver_identity(v)
    return ('general = { name = '+json.dumps(c['name'])+'; output_backend = "pipe"; port = '+str(c['port'])+';\n'
        'service_type = "airplay2"; ignore_volume_control = "yes"; airplay_device_id = 0x'+identity['device_id']+'L;\n'
        'nqptp_shared_memory_interface_name = '+json.dumps(identity['nqptp_interface'])+'; audio_backend_latency_offset_in_seconds = -0.87; };\n'
        'pipe = { name = '+json.dumps(str(fifo))+'; output_rate = 44100; output_format = "S16_LE"; output_channels = 2;\n'
        'timestamped = true; timestamped_write_timeout_ms = 50; startup_diagnostics = false; };\n'
        'diagnostics = { log_verbosity = 0; }; metadata = { enabled = "no"; };\n')
