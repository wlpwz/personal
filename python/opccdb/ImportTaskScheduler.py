import threading
import socket
import sys
import time

import common
from Thread import *
import Cli
import Settings

class SliceProcessState:
    TODO = 0
    IMPORTING = 1
    IMPORTED = 2
    NA = 127

class ScheduleData:
    def __init__(self):
        self.lock = threading.RLock()
        self.machine_list = {}
        self.slice2machines = {}
        self.slice_ids = []
        self.slice_list = {}
        self.slice_start = -1
        self.slice_end = -1
        self.slice_done_flag = {}

schedule_data = ScheduleData()

def show_sche():
    global schedule_data
    for key, value in schedule_data.machine_list.items():
        print key, value
    print '=' * 30
    sys.stdout.flush()


class ImportTaskScheduler(Thread):
    def __init__(self):
        Thread.__init__(self)
        global schedule_data
        schedule_data.lock.acquire()
        try:
            start = Settings.slice_start
            end = Settings.slice_end
            self.slices_and_stateCount = Cli.list_slice(Settings.import_repo)
            slices = self.slices_and_stateCount[0]
            slice_ids = []
            slice_machines = {}

            # get specified_slices info
            specified_slices = set()
            specified_flag = False
            f = open('import_specified_slices', 'r')
            for line in f:
                specified_slices.add(int(line))
            if len(specified_slices) > 0:
                specified_flag = True

            for s in slices:
                if start <= s.slice_id <= end:
                    if specified_flag:
                        if s.slice_id not in specified_slices:
                            continue
                    slice_ids.append(s.slice_id)
                    schedule_data.slice_done_flag[s.slice_id] = SliceProcessState.TODO
                    ret_list = Cli.show_slice(Settings.import_repo, s.slice_id)
                    slice_machines[s.slice_id] = []
                    for unit in ret_list[1]:
                        slice_machines[s.slice_id].append(unit.node)
            schedule_data.slice_ids = slice_ids
            schedule_data.slice2machines = slice_machines
            machines = Cli.list_server()
            for m in machines:
                schedule_data.machine_list[m.addr.ip+':'+str(m.addr.port)] = 0
#            fp = open('slice_done', 'r')
#            for line in fp:
#                n, state = line.strip().split(':')
#                if state == '0':
#                    state = SliceProcessState.TODO
#                elif state == '1':
#                    state = SliceProcessState.IMPORTING
#                elif state == '2':
#                    state = SliceProcessState.IMPORTED
#                else:
#                    raise Exception('Error at parsing file slice_done.')
#                schedule_data.slice_done_flag[int(n)] = state
        except:
            common.print_exc_plus()
        finally:
            schedule_data.lock.release()
    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('', Settings.schedule_port))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.listen(5)
        while True:
            try:
                client_socket, addr = s.accept()
                common._logger.info("get accepted at (ip:%s, port:%s)" % (addr[0], addr[1]))
                ScheduleSlice(client_socket).start()
            except:
                common.print_exc_plus()

class ScheduleSlice(Thread):
    def __init__(self, sock):
        self.sock = sock
        Thread.__init__(self)

    def run(self):
        try:
            count = 0
            buf = self.sock.recv(8)
            while len(buf) < 8 and count < 10:
                buf1 = self.sock.recv(8-len(buf), socket.MSG_DONTWAIT)
                buf += buf1
                time.sleep(1)
                count += 1
            if len(buf) < 8:
                common._logger.warn("recved less than 8 bytes:%s, exit." % buf)
                return
            else:
                common._logger.info("recved 8 bytes:%s" % buf)
            if buf[:3] == 'get':
                self.get_undo_slice()
            elif buf[:3] == 'end':
                n = int(buf[3:])
                self.finish_slice(n)
            elif buf[:3] == 'err':
                n = int(buf[3:])
                self.reschedule_slice(n)
            else:
                common._logger.info("error header in scheduler importer, close socket")
        except:
            common.print_exc_plus()
        finally:
            self.sock.close()

    def get_undo_slice(self):
        global schedule_data
        common._logger.info("to get a todo_slice")
        schedule_data.lock.acquire()
        try:
            min_average = 99999
            start = Settings.slice_start
            end = Settings.slice_end
            for slice_id in schedule_data.slice_ids:
                if start <= slice_id <= end and schedule_data.slice_done_flag[slice_id] == SliceProcessState.TODO:
                    tasks_num = 0
                    for node in schedule_data.slice2machines[slice_id]:
                        tasks_num += schedule_data.machine_list[node]
                    if tasks_num == 0:
                        best_slice = slice_id
                        min_average = 0
                        break
                    elif min_average > (tasks_num / len(schedule_data.slice2machines[slice_id])):
                        best_slice = slice_id
                        min_average = tasks_num / len(schedule_data.slice2machines[slice_id])
            if min_average == 99999:
                common._logger.info("All the slices have been set to import!")
                raise Exception("All the slices have been set to import")
            for node in schedule_data.slice2machines[best_slice]:
                schedule_data.machine_list[node] += 1
            schedule_data.slice_done_flag[best_slice] = SliceProcessState.IMPORTING
            best_slice = str(best_slice)
            l = len(best_slice)
            if l < 5:
                best_slice = '0'*(5-l) + best_slice
            common._logger.info('choose slice %s to import' % best_slice)
            self.sock.send(best_slice)
            show_sche()
        except:
            common.print_exc()
        finally:
            schedule_data.lock.release()

    def reschedule_slice(self, n):
        global schedule_data
        schedule_data.lock.acquire()
        try:
            if schedule_data.slice_done_flag[n] != SliceProcessState.IMPORTING:
                common._logger.info("slice %d isn't IMPORTING!" % n)
                return
            common._logger.info('slice %d error when importing.' % n)
            schedule_data.slice_done_flag[n] = SliceProcessState.TODO
            for node in schedule_data.slice2machines[n]:
                schedule_data.machine_list[node] -= 1
            show_sche()
        except:
            common.print_exc()
        finally:
            schedule_data.lock.release()

    def finish_slice(self, n):
        global schedule_data
        schedule_data.lock.acquire()
        try:
            if schedule_data.slice_done_flag[n] != SliceProcessState.IMPORTING:
                common._logger.info("slice %d isn't IMPORTING!" % n)
                return
            common._logger.info('slice %d finished import.' % n)
#            fp = open('slice_done', 'r+')
#            for line in fp:
#                n, state = line.strip().split(':')
#                schedule_data.slice_done_flag[int(n)] = state
            schedule_data.slice_done_flag[n] = SliceProcessState.IMPORTED
            for node in schedule_data.slice2machines[n]:
                schedule_data.machine_list[node] -= 1
            show_sche()
        except:
            common.print_exc()
        finally:
            schedule_data.lock.release()

