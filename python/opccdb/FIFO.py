import copy
import re

class FIFODest:
    def __init__(self, fifo, name, addr, held = False):
        self.fifo = fifo
        self.name = name
        self.addr = addr
        self.held = held

    def __str__(self):
        return 'fifo:%s, name:%s, addr:%s' % (self.fifo.name, self.name, self.addr.host)

class FIFO:
    def __init__(self, addr, name = '', held = False, machine = None):
        self.addr = addr
        self.name = name
        self.held = held
        self.dests = []
        self.machine = machine

    def add_dest(self, name, addr, held=False):
        self.dests.append(FIFODest(self, name, addr, held))
        return self.dests[-1]

    def remove_dest(self, addr):
        for i in range(len(self.dests)):
            if self.dests[i].addr == addr:
                del self.dests[i]
                return
        raise Exception('No dest for %s found' % str(addr))

    def get_dest(self, name):
        for dest in self.dests:
            if dest.name == name:
                return dest

    def get_dest_name(self, addr):
        for dest in self.dests:
            if dest.addr == addr:
                return dest.name
            
    def has_dest(self, addr):
        for dest in self.dests:
            if dest.addr == addr:
                return True
        return False

    def __eq__(self, other):
        return isinstance(other, FIFO) and self.addr == other.addr
    
    def __hash__(self):
        return hash(self.addr.host) ^ hash(self.addr.ip) ^ hash(self.addr.port)
    
class FIFOGroup:
    def __init__(self, name, base_port_shift, use_newdc=False, newdc_login_code="", newdc_num=1):
        if len(name) > 8 or len(name) < 1:
            raise Exception('length of fifo group name should be in the range of [1,8].')
        if re.compile(r'[\w\-]+$').match(name) is None:
            raise Exception('fifo group name is invalid, should use [a-zA-Z_-]+, but is [%s]' % name)
        self.name = name
        self.base_port_shift = int(base_port_shift)
        self.use_newdc = bool(use_newdc)
        self.newdc_login_code = newdc_login_code
        self.newdc_num = newdc_num
        # fifo.addr.port -> FIFO
        self.fifos = {}
    
    def add_fifo(self, fifo_addr, fifo = None):
        if fifo_addr.port not in self.fifos:
            if not fifo:
                fifo = FIFO(fifo_addr)
            self.fifos[fifo_addr.port] = fifo
            return fifo
        return self.fifos[fifo_addr.port]
    
    def remove_fifo(self, fifo_addr):
        if fifo_addr.port in self.fifos:
            self.fifos.pop(fifo_addr.port)
        raise Exception('fifo %s not found' % fifo_addr.long_key())
    
    def get_fifos(self):
        return copy.copy(self.fifos.values())
    
    def clone(self):
        return FIFOGroup(self.name, self.base_port_shift, self.use_newdc, self.newdc_login_code, self.newdc_num)
       
class FIFOMachine:
    def __init__(self, addr, tag):
        self.addr = addr
        self.fifos = [[], []]
        self.tag = tag

    def add_fifo(self, name):
        self.fifos.append(FIFO(self.addr, name))
        return self.fifos[-1]

    def get_fifo(self, name):
        for fifo in self.fifos:
            if fifo.name == name:
                return fifo
