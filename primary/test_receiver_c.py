from pathlib import Path
import os,socket,subprocess,tempfile,time,unittest
import session as o

class ReceiverC(unittest.TestCase):
    def test_real_c_packets_bind_volume_to_connection_generation(self):
        compiler=__import__('shutil').which('cc')
        if not compiler:self.skipTest('C compiler unavailable; run native C check separately')
        root=Path(__file__).parent/'receiver'
        program=r'''
#include "session_observer.h"
#include <stdlib.h>
int main(int argc,char **argv){
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
        with tempfile.TemporaryDirectory() as tmp:
            c=Path(tmp)/'h.c';c.write_text(program);binary=Path(tmp)/'h'
            subprocess.run([compiler,'-std=c11','-Wall','-Wextra','-I',str(root),str(c),str(root/'session_observer.c'),'-pthread','-lm','-o',str(binary)],check=True,capture_output=True)
            a,b=socket.socketpair(socket.AF_UNIX,socket.SOCK_DGRAM);a.settimeout(.2)
            try:
                proc=subprocess.Popen([str(binary),str(b.fileno())],pass_fds=(b.fileno(),));b.close();self.assertEqual(proc.wait(2),0)
                events=[]
                while True:
                    try:events.append(o.Event.parse(a.recv(100)))
                    except (socket.timeout,ConnectionResetError):break
            finally:a.close();b.close()
        begins=[e for e in events if e.event==2]
        self.assertEqual([(e.generation,e.volume_milli,e.volume_revision) for e in begins],[(1,-12000,1),(2,-3000,1),(3,0,0)])
        volumes=[e for e in events if e.event==7]
        self.assertEqual([(e.generation,e.volume_milli,e.volume_revision) for e in volumes],[(1,-9000,2),(1,-144000,3)])

if __name__=='__main__':unittest.main()
