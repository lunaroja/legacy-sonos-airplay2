"""Linux private receiver IPC and Driver wiring; no service/device I/O at import.

The launcher owns a per-child private FIFO, process and this channel. Never
reattach an observer to an already-running receiver or feed it another FIFO.
"""
from __future__ import annotations
import array
import os
import socket
import stat
import struct
from session import EVENT, Event, Observer


class SessionChannel:
    def __init__(self, observer: Observer):
        if not hasattr(socket,'SO_PASSCRED') or not hasattr(socket,'SCM_CREDENTIALS'):
            raise RuntimeError('linux_per_message_credentials_required')
        self.observer = observer
        self.reader,self.writer = socket.socketpair(socket.AF_UNIX,socket.SOCK_DGRAM)
        self.reader.setsockopt(socket.SOL_SOCKET,socket.SO_PASSCRED,1)
        self.reader.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,16384)
        self.reader.setblocking(False)
        self.child_fd = self.writer.fileno()
        info = os.fstat(self.child_fd)
        self.channel_identity = (info.st_dev,info.st_ino)
        self.proc = None
        self.fifo_fd = None
        self.fifo_identity = None
        self.closed = False
        self.failure = None

    def launch_options(self):
        if self.closed or self.proc is not None:
            raise RuntimeError('channel_not_launchable')
        fd = self.writer.fileno()
        # Only this descriptor is inherited. Never use close_fds=False.
        return {'pass_fds':(fd,), 'close_fds':True,
                'env_additions':{'SPS_SELECTION_FD':str(fd)}}

    @staticmethod
    def _fifo_identity(fd):
        info = os.fstat(fd)
        if not stat.S_ISFIFO(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('dedicated_private_fifo_required')
        return info.st_dev,info.st_ino

    def attach(self, proc, fifo_fd):
        if self.closed or self.proc is not None or type(proc.pid) is not int or proc.pid <= 1:
            raise ValueError('receiver_process_required')
        self.fifo_identity = self._fifo_identity(fifo_fd)
        self.fifo_fd = fifo_fd
        self.proc = proc
        self.writer.close()  # parent cannot accidentally act as receiver

    def _lineage_alive(self):
        # UNIX datagram sockets do not signal peer close. Check the original
        # inherited descriptor on the still-owned, unreaped child. A receiver
        # that closes its observer after an IPC failure cannot retain HOLD.
        try:
            if self.proc is None or self.proc.poll() is not None:return False
            info = os.stat(f'/proc/{self.proc.pid}/fd/{self.child_fd}')
            return (stat.S_ISSOCK(info.st_mode)
                    and (info.st_dev,info.st_ino)==self.channel_identity
                    and self._fifo_identity(self.fifo_fd)==self.fifo_identity)
        except (OSError,ValueError,TypeError):return False

    def observe_record(self, record, fifo_fd):
        try:
            if (self.closed or self.proc is None or self.proc.poll() is not None
                    or self._fifo_identity(fifo_fd) != self.fifo_identity):
                raise ValueError('fifo_owner_changed')
        except (OSError,ValueError):
            self.close('fifo_attachment_lost')
            return False
        if not self.observer.observe_record(record):
            self.close('fifo_record_rejected')
            return False
        return True

    def _packet(self):
        packet,ancillary,flags,_ = self.reader.recvmsg(EVENT.size+1,socket.CMSG_SPACE(128))
        credentials = []
        unsupported = False
        for level,typ,data in ancillary:
            if level == socket.SOL_SOCKET and typ == socket.SCM_CREDENTIALS and len(data) == 12:
                credentials.append(struct.unpack('=iii',data))
            else:
                unsupported = True
                if level == socket.SOL_SOCKET and typ == socket.SCM_RIGHTS:
                    fds = array.array('i');fds.frombytes(data[:len(data)-len(data)%fds.itemsize])
                    for fd in fds:
                        os.close(fd)
        if (unsupported or flags & (socket.MSG_TRUNC|socket.MSG_CTRUNC)
                or len(credentials) != 1 or credentials[0][:2] != (self.proc.pid,os.getuid())):
            raise ValueError('untrusted_event_packet')
        return packet

    def _lineage_lost(self):
        # The receiver can queue FAULT and close its inherited FD before the
        # reader wakes. Queued packets can explain the failure, never refresh
        # ownership or enter the Observer after lineage has been lost.
        reason = 'pinned_lineage_lost'
        for _ in range(32):
            try:
                packet = self._packet()
            except (OSError,ValueError):
                break
            try:
                event = Event.parse(packet)
            except ValueError:
                continue
            if event.event == 6 and event.flags == 16:
                reason = 'receiver_reported_fault'
                break
        self.close(reason)

    def drain(self):
        if self.closed or self.proc is None:
            return 0
        if not self._lineage_alive():
            self._lineage_lost()
            return 0
        count = 0
        for _ in range(32):
            try:
                packet = self._packet()
            except BlockingIOError:
                break
            except OSError:
                self.close('event_receive_failed')
                break
            except ValueError:
                self.close('untrusted_event_packet')
                break
            if not self.observer.feed(packet):
                self.close('receiver_reported_fault' if self.observer.failure == 'receiver_observer_failed'
                           else 'event_rejected')
                break
            count += 1
        if not self.closed:
            if self._lineage_alive():self.observer.lineage_checked()
            else:self._lineage_lost()
        return count

    def close(self, reason=None):
        if not self.closed:
            self.failure = reason
            self.closed = True
            self.reader.close();self.writer.close()
            self.observer.invalidate()
