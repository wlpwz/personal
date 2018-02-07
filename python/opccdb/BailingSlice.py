import Settings

class Slice():
    def __init__(self, content):
        items = content.split('|')
        self.slice_id = int(items[1])
        self.rep_revision = int(items[2])
        self.units_num = int(items[3])
        self.state = items[4].strip()

    def __str__(self):
        return 'SliceId(%d):rep_revision(%d),units(%d),state(%s)' % (
                self.slice_id, self.rep_revision, self.units_num, self.state)


class Unit():
    def __init__(self, content):
        items = content.split('|')
        self.table_id = int(items[1])
        self.slice_id = int(items[2])
        self.node = items[3].strip()
        self.unit_addr = items[4].strip()
        self.repl_addr = items[5].strip()
        self.revision = int(items[6])
        self.rep_revision = int(items[7])
        self.state = items[8].strip()
        if Settings.has_quota:
            self.mem_used_quota = items[9].strip()
            self.ssd_used = float(items[10])
            self.disk_used = float(items[11])
            self.quota_ssd_disk = items[12].strip()
            self.nPatches = int(items[13])
            self.nRecords = int(items[14])
        else:
            self.ssd_used = float(items[9])
            self.disk_used = float(items[10])
            self.nPatches = int(items[11])
            self.nRecords = int(items[12])

    def __str__(self):
        return 'Tableid(%d),SliceId(%d):node(%s),unitAddr(%s),replAddr(%s)' % (
                self.table_id, self.slice_id, self.node, self.unit_addr, self.repl_addr)


class Snapshot():
    def __init__(self, content):
        items = content.split('|')
        self.slice_id = int(items[1])
        self.name = items[2].strip()
        self.snapshot_id = int(items[3])
        self.create_time = items[4].strip()
        self.life_time = int(items[5])
        self.type = items[6].strip()
        self.state = items[7].strip()

    def __str__(self):
        return 'Slice(%d):name(%s),snapshot_id(%d),create_time(%s),life_time(%d),type(%s),state(%s)' % (
                self.slice_id, self.name, self.snapshot_id, self.create_time, 
                self.life_time, self.type, self.state)

