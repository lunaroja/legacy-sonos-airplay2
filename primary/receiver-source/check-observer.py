#!/usr/bin/env python3
"""Exercise generated SPSE v2 C code through an inherited local datagram FD."""
import argparse
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile

PROGRAM = r'''
#include "session_observer.h"
#include <stdlib.h>
int main(int argc,char **argv){
 (void)argc;
 int a=1,b=2; const char *gid="23a03d08-3ea9-42ba-b8f7-5299203b0de1";
 if(spobs_init_fd(atoi(argv[1])))return 2;
 spobs_volume_text(&a,"nan");spobs_volume_text(&a,"-12");
 spobs_initial(&a,gid);spobs_setup_result(&a,gid,200,0);spobs_begin(&a,gid,1);
 spobs_written(3,1,1,0,0,0);spobs_setup_result(&a,gid,200,0);
 spobs_volume_text(&a,"-9");spobs_volume_text(&a,"-9");
 spobs_volume_text(&a,"inf");spobs_volume_text(&a,"-1junk");
 spobs_volume_text(&a,"-31");spobs_volume_text(&a,"-144");spobs_volume_text(&b,"-3");spobs_forget(&a);
 spobs_initial(&b,gid);spobs_setup_result(&b,gid,200,0);spobs_begin(&b,gid,1);spobs_forget(&b);
 spobs_initial(&b,gid);spobs_setup_result(&b,gid,200,0);spobs_begin(&b,gid,1);spobs_forget(&b);
 return 0;
}
'''

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('generated', type=Path)
    args = parser.parse_args()
    generated = args.generated.resolve()
    wire = struct.Struct('!4sBBHQQQIIIIQII16siI')
    assert wire.size == 88
    with tempfile.TemporaryDirectory(prefix='observer-check-') as temp:
        source = Path(temp)/'test.c'; binary = Path(temp)/'test'
        source.write_text(PROGRAM)
        subprocess.run(['cc','-std=c11','-Wall','-Wextra','-I',str(generated),str(source),
                        str(generated/'session_observer.c'),'-pthread','-lm','-o',str(binary)],
                       check=True, capture_output=True, timeout=30)
        reader, writer = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        reader.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
        reader.settimeout(.2)
        process = None
        try:
            process = subprocess.Popen([str(binary), str(writer.fileno())], pass_fds=(writer.fileno(),))
            writer.close()
            assert process.wait(3) == 0
            events = []
            while True:
                try:
                    data, ancillary, flags, _ = reader.recvmsg(100, socket.CMSG_SPACE(12))
                except (socket.timeout, ConnectionResetError):
                    break
                assert not flags and len(data) == 88
                credentials = [struct.unpack('=iii', value) for level, kind, value in ancillary
                               if (level, kind) == (socket.SOL_SOCKET, socket.SCM_CREDENTIALS)]
                assert credentials == [(process.pid, os.getuid(), os.getgid())]
                row = wire.unpack(data)
                assert row[0:2] == (b'SPSE', 2)
                events.append(row)
        finally:
            if process is not None and process.poll() is None:
                process.kill(); process.wait()
            reader.close(); writer.close()
    assert [row[4] for row in events] == list(range(1, len(events)+1))
    assert [(r[5], r[-2], r[-1]) for r in events if r[2] == 2] == [(1,-12000,1),(2,-3000,1),(3,0,0)]
    assert [(r[5], r[-2], r[-1]) for r in events if r[2] == 7] == [(1,-9000,2),(1,-144000,3)]
    print(json.dumps({'spse_version': 2, 'event_bytes': 88, 'events': len(events),
                      'connection_volume_generations_verified': 3,
                      'kernel_pid_uid_gid_verified': True, 'live_playback_verified': False}))

if __name__ == '__main__':
    main()
