import copy
import socket
import threading
import traceback

from Thread import *
import common

class Addr:
    def __init__(self, host, ip, port):
        self.host = host
        self.ip = ip
        self.port = int(port)
        self.host_key = self.host + ':' + str(self.port)
        self.ip_key = self.ip + ':' + str(self.port)

    def __eq__(self, other):
        return isinstance(other, Addr) and (self.host == other.host or self.ip == other.ip) and self.port == other.port

    def __str__(self):
        return '%s(%s):%d' % (self.host, self.ip, self.port)

    def __hash__(self):
        return hash(self.host) ^ hash(self.ip) ^ hash(self.port)
        
    def long_key(self):
        return '%s(%s):%d' % (self.host, self.ip, self.port)
    
_cache = {}
_lock = threading.Lock()
NoAddr = Addr('NA', '0.0.0.0', 0)

def _resolve(host_or_ip, port):
    try:
        host = socket.getfqdn(host_or_ip)
        ip = socket.getaddrinfo(host, port)[0][4][0]
        return Addr(host, ip, port)
    except:
        return NoAddr
    
def refresh():
    new_cache = {}
    global _cache
    _lock.acquire()
    try:
        ori_cache = _cache.copy()
    finally:
        _lock.release()
    for host_or_ip in ori_cache:
        if host_or_ip not in new_cache:
            addr = _resolve(host_or_ip, 0)
            new_cache[host_or_ip] = addr
            new_cache[addr.host] = addr
            new_cache[addr.ip] = addr
    _cache = new_cache

class DNSUpdater(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        while not self.interrupt_flag:
            try:
                refresh()
                self.sleep(900)
            except:
                if sys.exc_info()[1] == 514:
                    pass
                common.print_exc()
DNSUpdater().start()

def resolve(host_or_ip, port):
    _lock.acquire()
    try:
        if host_or_ip in _cache:
            ret = copy.copy(_cache[host_or_ip])
            ret.port = port
            return ret
    finally:
        _lock.release()
    addr = _resolve(host_or_ip, port)
    _lock.acquire()
    try:
        _cache[host_or_ip] = addr
        _cache[addr.host] = addr
        _cache[addr.ip] = addr
        return addr
    finally:
        _lock.release()

def parse(addr_string):
    host_or_ip, port = addr_string.split(':')
    port = int(port)
    return resolve(host_or_ip, port)
