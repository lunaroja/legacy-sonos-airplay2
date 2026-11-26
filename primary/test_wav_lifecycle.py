"""An owned WAV connection can be revoked while its worker checks the socket."""
import select,socket,threading,unittest
from unittest.mock import patch
from urllib.parse import urlsplit
from wav_http import Wave

class WavLifecycle(unittest.TestCase):
    def test_revoke_between_socket_check_and_select_closes_without_worker_error(self):
        relay=Wave('127.0.0.1',0,'127.0.0.1',present=lambda:True)
        relay.start();self.addCleanup(relay.close)
        uri=relay.activate('one');relay.push('one',bytes(64))
        entered=threading.Event();release=threading.Event();finished=threading.Event();errors=[]
        original_select=select.select;original_worker=relay.server.process_request_thread
        relay.server.handle_error=lambda *_:errors.append(True)
        def completed(*args):
            try:original_worker(*args)
            finally:finished.set()
        relay.server.process_request_thread=completed
        def interleave(readers,writers,exceptions,timeout):
            if readers and readers[0] in relay.clients and not entered.is_set():
                entered.set()
                if not release.wait(2):raise RuntimeError('test_release_missing')
            return original_select(readers,writers,exceptions,timeout)
        with patch.object(select,'select',side_effect=interleave):
            with socket.create_connection(('127.0.0.1',relay.port),timeout=2) as client:
                client.sendall(('GET '+urlsplit(uri).path+' HTTP/1.1\r\nHost: localhost\r\n\r\n').encode())
                try:
                    self.assertTrue(entered.wait(2),'WAV worker never reached the live socket check')
                    relay.revoke()
                finally:release.set()
                self.assertTrue(finished.wait(2),'Revoked worker did not finish')
        self.assertEqual(errors,[])

if __name__=='__main__':unittest.main()
