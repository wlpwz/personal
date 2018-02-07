import common
import threading
import time
import socket
import sys

import DNS
import Settings

class MachineType:
    OFFLINE = 0
    FIFO = 1
    WRITEPROXY = 2
    BLACK_LIST = 6
    # last one, for validation, not a valid value
    NA = 255
    
class Machine:
    def __init__(self, content):
#        common._logger.info('machine:%s', content)
        if Settings.has_quota:
            self.init_quota(content)
        else:
            self.init(content)

    def init(self,content):
        items = content.split('|')
        self.node = items[1].strip()
        ip, port = items[2].strip().split(':')
        self.addr = DNS.resolve(ip, port)
        self.status = items[3].strip()
        self.units = items[4].strip().split('/')
        self.primary_units = items[5].strip().split('/')
        self.CpuIdle = int(items[6])
        self.iopses = items[7].strip().split('/')
        self.throughputs = items[8].strip().split('/')
        self.mems = items[9].strip().split('/')
        self.ssd_spaces = items[10].strip().split('/')
        self.disk_spaces = items[11].strip().split('/')

    def init_quota(self,content):
        items = content.split('|')
        self.node = items[1].strip()
        ip, port = items[2].strip().split(':')
        self.addr = DNS.resolve(ip, port)
        self.status = items[3].strip()
        self.units = items[4].strip().split('/')
        self.primary_units = items[5].strip().split('/')
        self.CpuIdle = int(items[6])
        self.iopses = items[7].strip().split('/')
        self.throughputs = items[8].strip().split('/')
        self.mems = items[9].strip().split('/')
        self.ssd_spaces = items[10].strip().split('/')
        self.disk_spaces = items[11].strip().split('/')
        self.mem_quota = items[12].strip().split('/')
        self.disk_quota = items[13].strip().split('/')

class WriteProxy:
    def __init__(self, ip):
        self.ip = ip
        self.last_check_time = 0
#        self.fifo_ref_count = 0
        self.dests = [ [], [] ]
        self.valid_dest_count = [0, 0] # including those to be build

#_machines = {}
#
#def get_machines():
#    return _machines.values()


class WriteProxyListener(threading.Thread):
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        common._logger.info("started WriteProxyListener")

    def run(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', self.port))
            s.listen(20)
            while True:
                client_sock, addr = s.accept()
#                common._logger.debug("get accepted at (ip:%s, port:%s)" % (addr[0], addr[1]))
                WPInfo(client_sock, addr[0]).start()
        except:
            common.print_exc()


class WPInfo(threading.Thread):
    def __init__(self, sock, ip):
        threading.Thread.__init__(self)
        self.sock = sock
        self.ip = ip

    def run(self):
        try:
            packlen = 32
            count = 0
            buf = self.sock.recv(packlen)
            while len(buf) < packlen and count < 10:
                buf1 = self.sock.recv(packlen-len(buf), socket.MSG_DONTWAIT)
                buf += buf1
                time.sleep(1)
                count += 1
            if len(buf) < packlen:
                common._logger.info("recved less than %d bytes in 10 seconds:%s, exit." % (packlen, buf))
                return
            else:
                pass
            self.addr = DNS._resolve(self.ip, 0)
            global machine_lock
            machine_lock.acquire()
            try:
                if buf[:4] == 'live':
                    repo=buf.split(' ')[0][4:]
                    ip=buf.split(' ')[1]
                    self.live(repo, ip)
                elif buf[:4] == 'dead':
                    pass
                else:
                    common._logger.info("error header in scheduler importer, close socket")
            finally:
                machine_lock.release()
        except:
            common.print_exc_plus()
        finally:
            self.sock.close()

    def live(self, repo, ip):
        self.ip = ip
        if not repo_writeproxys.has_key(repo):
            repo_writeproxys[repo] = set()
        else:
            if writeproxys.has_key(self.ip):
                writeproxys[self.ip].last_check_time = int(time.time())
            else:
                wp = WriteProxy(self.ip)
                wp.last_check_time = int(time.time())
                repo_writeproxys[repo].add(self.ip)
                writeproxys[self.ip] = wp
        #debug
        for ip in writeproxys.keys():
            print '[detect writeproxy live ip:%s]'%ip

    def dead(self, repo):
        if not repo_writeproxys.has_key(repo):
            common._logger.info("repo(%s)'s writeproxys don't exist yet, ignore dead message." % repo)
        else:
            if writeproxys.has_key(self.ip):
                writeproxys.pop(self.ip)
            else:
                common._logger.info("%s(%s) is not in writeproxys." % (self.addr.host, self.ip))
            if repo_writeproxys[repo].has_key(self.ip):
                repo_writeproxys[repo].pop(self.ip)
            else:
                common._logger.info("%s(%s) is not in repo(%s)'s writeproxys." % (self.addr.host, self.ip, repo))

machine_lock = threading.Lock()

repo_writeproxys = {} # repo -> set(ip, ip, ...)
writeproxys = {} # ip -> WriteProxy

repo_fifos = {} # repo -> set(ip, ip, ...)
fifo_machines = {} # ip -> Fifo_machine


