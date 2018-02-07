import common
import Settings

class Repository():
    def __init__(self, content):
        if Settings.has_quota:
            items = content.split('|')
            self.name = items[1].strip()
            self.table_id = int(items[2])
            self.revision = int(items[3])
            self.rd_token = items[4].strip()
            self.rw_token = items[5].strip()
            self.replica_num = int(items[6])
            self.mem_quota = items[7].strip()
            self.disk_quota = items[8].strip()
            self.bailing_mode = items[9].strip()
            self.partitions = int(items[10])
            self.state = items[11].strip()
            self.slice_num = int(items[12])
        else:   
            items = content.split('|')
            self.name = items[1].strip()
            self.table_id = int(items[2])
            self.revision = int(items[3])
            self.rd_token = items[4].strip()
            self.rw_token = items[5].strip()
            self.replica_num = int(items[6])
            self.partitions = int(items[7])
            self.state = items[8].strip()
            self.slice_num = int(items[9])
        # fifo
        self.fifo_groups = {}
#        self.lock = threading.RLock()

    def update_fifos(self):
        pass
#        self.lock.acquire()
#        try:
#            if int(time.time()) < self.last_update_fifo_time + 10:
#                return
#            self.last_update_fifo_time = int(time.time())
#            fifo_groups = {}
#            for fifo_group in self.fifo_groups.values():
#                plow = self.base_port + fifo_group.base_port_shift
#                phi = plow + self.cache_num
#                for i in xrange(plow, phi):
#                    fifo_groups[fifo_group.name] = fifo_group.clone()
#            for machine in get_machines_by_tagtype(self.name, MachineType.FIFO):
#                if machine.status and machine.status.fifo:
#                    for fifo in machine.status.fifo.fifos:
#                        if fifo.machine != machine:
#                            fifo.machine = machine
#                        for fifo_group in fifo_groups.values():
#                            plow = self.base_port + fifo_group.base_port_shift
#                            phi = plow + self.cache_num
#                            if fifo.addr.port >= plow and fifo.addr.port < phi:
#                                fifo.addr = DNS.resolve(machine.addr.ip, fifo.addr.port)
#                                fifo_group.add_fifo(fifo.addr, fifo)
#            self.fifo_groups = fifo_groups
#        finally:
#            self.lock.release()
