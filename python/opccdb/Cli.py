import common
import MachineManager
import BailingRepository
import BailingSlice

g_slice_states = ['normal','creating','altering','dropping','creating snapshot','deleting snapshot','balancing']

def list_server():
    machine_list = []
    try:
        shell_out = common.run_shell('echo "list server;" | ./Cli -s')
        output_lines = shell_out.split('\n')
        n = len(output_lines)
        if len(output_lines) > 7:
            status = output_lines[7].split('|')[3].strip()
            if status != 'ok':
                pass # TODO
            else:
                for line_idx in range(13, n, 2):
                    if output_lines[line_idx] != '':
                        machine_list.append(MachineManager.Machine(output_lines[line_idx]))
        return machine_list
    except:
        common.print_exc_plus()
	
def list_table():
    "ret list: [Repository]"
    table_list = []
    try:
        shell_out = common.run_shell('echo "list table;" | ./Cli -s')
        output_lines = shell_out.split('\n')
        n = len(output_lines)
        if len(output_lines) > 7:
            status = output_lines[7].split('|')[3].strip()
            if status != 'ok':
                pass # TODO
            else:
                for line_idx in range(13, n, 2):
                    stmp = output_lines[line_idx]
                    if stmp != '':
                        table_list.append(BailingRepository.Repository(stmp))
        return table_list
    except:
        common.print_exc_plus()

def list_slice(table):
    "ret list: [Slice]"
    slice_and_state_count = [[],{},{}] # [[slice_list],state->num,state->slice_list]
    global g_slice_states
    for state in g_slice_states:
        slice_and_state_count[1][state] = 0
        slice_and_state_count[2][state] = []

    try:
        shell_out = common.run_shell('echo "list slice on table %s;" | ./Cli -s' % table)
        output_lines = shell_out.split('\n')
        n = len(output_lines)
        if len(output_lines) > 7:
            status = output_lines[7].split('|')[3].strip()
            if status != 'ok':
                pass # TODO
            else:
                for line_idx in range(13, n, 2):
                    stmp = output_lines[line_idx]
                    if stmp != '':
                        slice = BailingSlice.Slice(stmp)
                        slice_and_state_count[1][slice.state] += 1
                        slice_and_state_count[2][slice.state].append(slice)
                        slice_and_state_count[0].append(slice)
        return slice_and_state_count
    except:
        common.print_exc_plus()

def list_snapshot(table):
    ret_list = []
    try:
        shell_out = common.run_shell('echo "list snapshot on table %s;" | ./Cli -s' % table)
        output_lines = shell_out.split('\n')
        n = len(output_lines)
        if len(output_lines) > 7:
            status = output_lines[7].split('|')[3].strip()
            if status != 'ok':
                pass # TODO
            else:
                for line_idx in range(13, n, 2):
                    stmp = output_lines[line_idx]
                    if stmp != '':
                        l = stmp.split('|')[1:-1]
                        ret_list.append(l)
        return ret_list
    except:
        common.print_exc_plus()

def show_slice(table, slice_no):
    "ret_list: [Slice, unit_list, snapshot_list]"
    ret_list = []
    try:
        shell_out = common.run_shell('echo "show slice %d on table %s;" | ./Cli -s' % (int(slice_no), table))
        output_lines = shell_out.split('\n')
        n = len(output_lines)
        if len(output_lines) > 7:
            status = output_lines[7].split('|')[3].strip()
            if status != 'ok':
                pass # TODO
            else:
                sli = BailingSlice.Slice(output_lines[13])
                ret_list.append(sli)

#                rep_base = snapshot_base = 0
                for idx, line in enumerate(output_lines):
                    line = line.strip()
                    if line == 'Replication List:':
                        rep_base = idx
                    elif line == 'Snapshot List:' or line == 'Snapshot list:':
                        snapshot_base = idx
                rep_list = []
                for line_idx in range(rep_base + 4, snapshot_base, 2):
                    stmp = output_lines[line_idx]
                    if stmp != '':
                        rep_list.append(stmp.split('|')[1].strip())

                unit_list = []
                for line_idx in range(19, rep_base, 2):
                    stmp = output_lines[line_idx]
                    if stmp != '':
                        unit = BailingSlice.Unit(stmp)
                        try:
                            unit.rep_order = rep_list.index(unit.repl_addr)
                        except:
                            unit.rep_order = 999
                        unit_list.append(unit)
                ret_list.append(unit_list)

                snapshot_list = []
                for line_idx in range(snapshot_base + 4, len(output_lines), 2):
                    stmp = output_lines[line_idx]
                    if stmp != '':
                        snapshot_list.append(BailingSlice.Snapshot(stmp))
                ret_list.append(snapshot_list)
        return ret_list
    except:
        common.print_exc_plus()

def show_server(node):
    ret_list = []
    try:
        shell_out = common.run_shell('echo "show server %s;" | ./Cli -s' % node)
        output_lines = shell_out.split('\n')
        n = len(output_lines)
        if len(output_lines) > 7:
            status = output_lines[7].split('|')[3].strip()
            if status != 'ok':
                pass # TODO
            else:
                for line_idx in range(19, len(output_lines), 2):
                    stmp = output_lines[line_idx]
                    if stmp != '':
                        ret_list.append(stmp.split('|')[1:-1])
        return ret_list
    except:
        common.print_exc_plus()

def show_snapshot(table, ss):
    ret_list = []
    try:
        shell_out = common.run_shell('echo "show snapshot %s on table %s;" | ./Cli -s' % (ss, table))
        output_lines = shell_out.split('\n')
        n = len(output_lines)
        if len(output_lines) > 7:
            status = output_lines[7].split('|')[3].strip()
            if status != 'ok':
                pass # TODO
            else:
                for line_idx in range(13, len(output_lines), 2):
                    stmp = output_lines[line_idx]
                    if stmp != '':
                        ret_list.append(stmp.split('|')[1:-1])
        return ret_list
    except:
        common.print_exc_plus()


if __name__ == '__main__':
#    list_server()
#    list_table()
#    l = show_slice('bailing', 0)
#    for e in l:
#        print e
#    for e in list_snapshot('bailing'):
#        print e
#    for e in show_server('2783752378704789504'):
#        print e
    for e in show_snapshot('bailing', 'table00000001_sys_fast_snapshot_000000000000000b'):
        print e
