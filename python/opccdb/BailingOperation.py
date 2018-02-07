# -*- coding: GB18030 -*-

import sys
import traceback
import subprocess
import os
import fcntl
import threading
import time
import re
import copy
import heapq

import DNS
import common
#from common import *
from HttpServer import *
import MachineManager
#from Mediator import *
from Thread import *
from BailingRepository import *
import Settings
from ErrorReporter import *
import ImportTaskScheduler
import FIFO

import Cli

import Ufeed

class HTTPRequestHandler:
    def default(self):
        self.redirect('/machine/all')

    def print_header(self):
        self.write('''<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=gbk" /> 
<title>新存储运维系统</title>
<style type="text/css">
<!--
.tab {
    width: 150px;
    padding:0
}
.tab_separator {
    width: 10px;
}
.active {
    background-color: #87cefa;
}
.inactive {
    background-color: #e0f0f9;
}
.tab_inner {
    margin:0;
    padding:5px 30px;
}
#items A:link {
    color:blue;
}
#items A:visited {
    color:blue;
}
.tab_inner A {
    color:black;
    text-decoration:none
}
.round4 {
    overflow:hidden;
    font-size:0;
    margin:0 4px;
    padding:0;
    height:1px;
}
.round2 {
    overflow:hidden;
    font-size:0;
    margin:0 2px;
    padding:0;
    height:1px;
}
.round1 {
    overflow:hidden;
    font-size:0;
    margin:0 1px;
    padding:0;
    height:1px;
}
.item {
    background:inherit;
    padding:5px 10px;
    float:left;
}
.normal {
    background-color:#aff0ff
}
.warning {
    background-color:#ffff00
}
.error {
    background-color:#ff0000
}
-->
</style>
<body>''')
        self.write('<H1 align=center>新存储运维系统</H1>')
        self.write('<H3 align=center>version %s</H3>' % Settings.version)
        pc = self.read_cookie('passcode')
        if pc is not None and pc != '':
            self.write('welcome, %s<br>' % pc)
        else:
            self.write('<a href="/op/login">login</a><br>')
        
    def print_tab(self, active):
        tabs = [('Machines', '/machine/all'), ('Slices', '/slice/all'), ('Snapshots', '/snapshot/all'), ('FIFO', '/fifo/all'), ('Repository', '/repository/all'), ('UnifiedFeed', '/ufeed/all'), ('WriteProxy', '/writeproxy/all'), ('Op', '/op/all')]
        self.write('''<div>
    <table border=0 cellpadding=0 cellspacing=0>
        <tr>''')
        for i in range(len(tabs)):
            if i > 0:
                self.write('<th class=tab_separator>')
            if i == active:
                inner = '<a href="%s">%s</a>' % (tabs[i][1], tabs[i][0]) #tabs[i][0]
            else:
                inner = '<a href="%s">%s</a>' % (tabs[i][1], tabs[i][0])
            self.write('''<th class=tab>
                <div class="round4 %(active)s"></div>
                <div class="round2 %(active)s"></div>
                <div class="round1 %(active)s"></div>
                <div class="tab_inner %(active)s">%(inner)s</div>''' % {'active' : i == active and 'active' or 'inactive', 'inner' : inner})
        self.write('''</table></div>''')

    def print_items(self, items):
        if items is None:
            items = []
        self.write('<div id=items class=active style="width: 100%; overflow: auto;">')
        for item in items:
            if item[1]:
                self.write('<div class=item><a href="%s">%s</a></div>' % (item[1], item[0]))
            else:
                self.write('<div class=item>%s</div>' % item[0])
        self.write('</div>')
        self.write('<div style="height:15px"></div>')

    def check_authorization(self, display = True):
        if self.read_cookie('passcode') != Settings.op_passcode:
            if display:
                self.write('check authorization failed for %s' % self.read_cookie('passcode'))
            return False
        return True

def get_mirror_display(mirror):
    if mirror.corrupted:
        return '损坏'
    elif mirror.data_loss:
        return '数据丢失'
    else:
        return '正常'

def get_status_string(machine):
    if not machine:
        return 'agent-fail'
    if machine.status == 'normal':
        return '正常'
    return '错误'

def machine_key(machine, key):
    try:
        if key == 0:
            return machine.addr.host
        if key == 1:
            return machine.addr.ip
        if key == 2:
            return get_status_string(machine)
        if key == 3:
            return int(machine.units[0])
        if key == 4:
            return int(machine.primary_units[0])
        if key == 5:
            return int(machine.CpuIdle)
        if key == 6:
            return float(machine.iopses[0])
        if key == 7:
            return float(machine.iopses[1])
        if key == 8:
            return float(machine.throughputs[0])
        if key == 9:
            return float(machine.throughputs[1])
        if key == 10:
            return float(machine.mems[0])
        if key == 11:
            return float(machine.mems[1])
        if key == 12:
            return float(machine.ssd_spaces[0])
        if key == 13:
            return float(machine.ssd_spaces[1])
        if key == 14:
            return float(machine.disk_spaces[0])
        if key == 15:
            return float(machine.disk_spaces[0])
        if Settings.has_quota:
            if key == 16:
                return float(machine.mem_quota[0])
            if key == 17:
                return float(machine.mem_quota[1])
            if key == 18:
                return float(machine.disk_quota[0])
            if key == 19:
                return float(machine.disk_quota[1])
    except:
        return -1
    
class MachineHandler(HTTPRequestHandler):
        
    def get_machines(self):
        try:
            if not os.path.exists(Settings.dump_machine_dir):
                os.system('mkdir -p %s &>/dev/null' % Settings.dump_machine_dir)
            bailing_machines = MachineManager.get_machines_by_type(MachineManager.MachineType.BAILING)
            if len(bailing_machines) > 0:
                f = open('%s/bailing' % Settings.dump_machine_dir, 'w')
                for mac in bailing_machines:
                    f.write('%s\n' % mac.addr.host)
                f.close()
            fifo_machines = MachineManager.get_machines_by_type(MachineManager.MachineType.FIFO)
            if len(fifo_machines) > 0:
                f = open('%s/fifo' % Settings.dump_machine_dir, 'w')
                for mac in fifo_machines:
                    f.write('%s\n' % mac.addr.host)
                f.close()
        finally:
            self.write('Done')
        
    def all(self, sort='-1'):
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 0)
        items = [['OFFLINE(%d)', '/machine/offline'], ['Online(%d)', '/machine/all']]
        items[1][1] = ''
        count = [0] * len(items)
        
        machines = Cli.list_server()
        count[1] = len(machines)

        for i in range(len(items)):
            items[i][0] = items[i][0] % count[i]
        HTTPRequestHandler.print_items(self, items)
        sort=int(sort)
        if sort >= 0:
            machines.sort(key=lambda x : machine_key(x, sort))
        options = ['机器名', 'IP', '状态', 'Units', 'Primary Units', '空闲cpu',
                   'SSD IOPS', 'Disk IOPS', 'SSD吞吐', 'Disk吞吐', 'Memory Used',
                   'Memory Total', 'SSD Used', 'SSD Total', 'Disk Used', 'Disk Total']
        self.write('''<div>
    <center>
    <form action="/machine/all">
        排序
        <select name=sort>''' + ''.join(['<option value=%d %s>%s</option>' % (i, sort == i and 'selected' or '', options[i]) for i in range(len(options))]) + '''</select>
        <input type=submit value='确定'>
    </form>
    </center>
    <table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
        <tr class=active>
            <th width=200px id=host>机器名
            <th width=150px id=ip>IP:Port
            <th width=50px id=status>状态
            <th width=100px id=input_output>Units
            <th width=150px id=cache_mem>Primary Units
            <th width=80px id=statistics>空闲cpu
            <th width=100px id=top_disk_usage>IOPS
            <th width=100px id=caches>吞吐(MB/s)
            <th width=100px id=tags>Memory(MB)
            <th width=100px id=cacheslink>SSD(GB)
            <th width=100px id=error_task_num>Disk(GB)
            ''')
        if Settings.has_quota:
            self.write('''<th width=150px id=mem_quota_left_cap>Mem Quota(MB)
                    <th width=150px id=disk_quota_left_cap>Disk Quota(GB)''')
        for machine in machines:
            st = machine.status
            self.write('<tr class=%s>' % st)
            self.write('<td nowrap><a href="/machine/one?node=%s&machine=%s">%s</a>'
                    % (machine.node, remove_baidu_tail(machine.addr.host), 
                        remove_baidu_tail(machine.addr.host)))
            self.write('<td nowrap>%s:%s' % (machine.addr.ip, machine.addr.port))
            self.write('<td nowrap>%s' % get_status_string(machine))
            self.write('<td>%s/%s<br>' % tuple(machine.units))
            self.write('<td>%s/%s<br>' % tuple(machine.primary_units))
            self.write('<td>%s%%<br>' % machine.CpuIdle)
            self.write('<td>SSD: %s<br>Disk: %s' % tuple(machine.iopses))
            self.write('<td nowrap>SSD: %s<br>Disk: %s' % tuple(machine.throughputs))
            self.write('<td nowrap>Used %s<br>Total %s' % tuple(machine.mems))
            self.write('<td nowrap>Used %s<br>Total %s' % tuple(machine.ssd_spaces))
            self.write('<td nowrap>Used %s<br>Total %s' % tuple(machine.disk_spaces))
            if Settings.has_quota:
                self.write('<td nowrap>Left %s<br>Allocated %s' % tuple(machine.mem_quota))
                self.write('<td nowrap>Left %s<br>Allocated %s' % tuple(machine.disk_quota))
        self.write('</table></div>')

    def one(self, node, machine):
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 0)
        self.write('<h4 f=head align=center>Machine %s</h4>' % machine)
        units_info = Cli.show_server(node)
        if Settings.has_quota:
            self.one_for_quota(units_info)
        else:
            self.write('''<div>
        <table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
            <tr class=active>
                <th width=60px id=tid>TableID
                <th width=60px id=sid>SliceID
                <th width=150px id=unit_addr>UnitAddr
                <th width=150px id=repl_addr>ReplAddr
                <th width=40px id=revision>Revision
                <th width=40px id=rep_revision>RepRevision
                <th width=50px id=state>State
                <th width=60px id=ssd_used>SSD_Used
                ''')
            global tables_info
            for unit in units_info:   #/slice/slice?repo=bailing&slice_no=4
                self.write('<tr class=normal>')
                self.write('<td>%d<br>' % int(unit[0]))
                self.write('<td><a href=/slice/slice?repo=%s&slice_no=%d>%d</a>'
                        % (tables_info[int(unit[0])], int(unit[1]), int(unit[1])))
                self.write('<td>%s<br>' % unit[3].strip())
                self.write('<td>%s<br>' % unit[4].strip())
                self.write('<td>%d<br>' % int(unit[5]))
                self.write('<td>%d<br>' % int(unit[6]))
                self.write('<td>%s<br>' % unit[7].strip())
                self.write('<td>%f<br>' % float(unit[8]))
            self.write('</table></div>')

    def one_for_quota(self, units_info):
        self.write('''<div>
<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=60px id=tid>TableID
<th width=60px id=sid>SliceID
<th width=150px id=unit_addr>UnitAddr
<th width=150px id=repl_addr>ReplAddr
<th width=40px id=revision>Revision
<th width=40px id=rep_revision>RepRevision
<th width=50px id=state>State
<th width=200px id=mem_quota_left_cap>Quota
<th width=100px id=ssd_used>SSD Used
<th width=150px id=disk_used>Disk Used(GB) ''')
        global tables_info
        for unit in units_info:   #/slice/slice?repo=bailing&slice_no=4
            self.write('<tr class=normal>')
            self.write('<td align="center">%d<br>' % int(unit[0]))
            self.write('<td align="center"><a href=/slice/slice?repo=%s&slice_no=%d>%d</a>'
                    % (tables_info[int(unit[0])], int(unit[1]), int(unit[1])))  #SliceID
            self.write('<td align="center">%s<br>' % unit[3].strip())           #UnitAddr
            self.write('<td align="center">%s<br>' % unit[4].strip())           #ReplAddr
            self.write('<td align="center">%d<br>' % int(unit[5]))              #Revision
            self.write('<td align="center">%d<br>' % int(unit[6]))              #RepRevision
            self.write('<td align="center">%s<br>' % unit[7].strip())           #State
            self.write('<td align="center">Mem used/Quota(MB):%s<br>Ssd+Disk(GB):%s' % (unit[8], unit[11]))   #Mem used/Quota(MB)Quota(Ssd+Disk,GB)
            self.write('<td align="center">%s<br>' % unit[9])                   #Ssd used
            self.write('<td align="center">%s<br>' % unit[10])                  #disk used
        self.write('</table></div>')

class SliceHandler(HTTPRequestHandler):
    def __init__(self):
        self.slices_rep = [[],[]]

    def print_items(self, repo, active):
        self.slices_and_state_count = Cli.list_slice(repo)
        self.slices = self.slices_and_state_count[0]
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 1)
        items = [['全部(%d)' % len(self.slices), '/slice/all?repo=%s' % repo]]
        items[active][1] = ''
        HTTPRequestHandler.print_items(self, items)

    def all(self, repo = None, page='0'):
         if (repo is None): 
             self.redirect('/repository/all')
             return
         self.print_items(repo, 0)
         self._show_page_index(repo, page)
         self._show_state_info_count(repo, self.slices_and_state_count)
         self.write('<br>')
         self._show_slices(repo, self.slices, page)
 
    def _show_page_index_for_state(self, repo, stat, page):
        page = int(page)
        if len(self.slices_and_state_count[2][stat]) > 0:
            total_pages = len(self.slices_and_state_count[2][stat])/200 + 1 
        else:
            total_pages = 0
        if page < 0:
            page = 0
        if page > total_pages:
            page = total_pages
        self.write('<div style="width:100%">')
        if total_pages > 1:
            self.write('<center>%s</center>' % '&nbsp;&nbsp;'.join([i == page and ('%s' %i) or ('<a href=/slice/state?repo=%s&state=%s&page=%d>%d</a>' % (repo, stat, i, i)) for i in range(total_pages)]))

    def _show_page_index(self, repo, page):
        page = int(page)
        if len(self.slices) > 0:
            total_pages = self.slices[-1].slice_id / 1000 + 1
        else:
            total_pages = 0
        if page < 0:
            page = 0
        if page > total_pages:
            page = total_pages
        self.write('<div style="width:100%">')
        if total_pages > 1:
            self.write('<center>%s</center>' % '&nbsp;&nbsp;'.join([i == page and ('%s' %i) or ('<a href=/slice/all?repo=%s&page=%d>%d</a>' % (repo, i, i)) for i in range(total_pages)]))
    def _show_rep_count(self, repo , slices_and_state_count):
        self.write('<h3 align=center>Slices\' Rep Count in %s</h3>' % repo)
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=100px nowrap id=one>1 Reps
<th width=100px nowrap id=two>2 Reps''')
        self.write('<tr class=normal>')
        for slice in slices_and_state_count[0]:
            if slice.units_num == 1 or slice.units_num == 2:
                self.slices_rep[slice.units_num-1].append(slice)
        rep1 = len(self.slices_rep[0])
        rep2 = len(self.slices_rep[1])
        if rep1 == 0:
            self.write('<td align="center" nowrap>0')
        else:
            self.write('<td align="center" nowrap><a href=/slice/rep_slices?repo=%s&rep=%d>%d</a>'%(repo, 1, rep1))
        if rep2 == 0:
            self.write('<td align="center" nowrap>0')
        else:
            self.write('<td align="center" nowrap> <a href=/slice/rep_slices?repo=%s&rep=%d>%d</a>'%(repo, 2, rep2))
        self.write('</table></div>')

    def rep_slices(self, repo, rep, page='0'):
        rep = int(rep)
        self.print_items(repo, 0)
        for slice in self.slices_and_state_count[0]:
            if slice.units_num == 1 or slice.units_num == 2:
                self.slices_rep[slice.units_num-1].append(slice)
        self._show_page_index_for_rep(repo, self.slices_rep[rep-1], page)
        self._show_slices_for_rep(repo, self.slices_rep[rep-1], page)

    def _show_page_index_for_rep(self, repo, slices, page):
        page = int(page)
        if len(slices) > 0:
            total_pages = len(slices)/200 
        else:
            total_pages = 0
        if page < 0:
            page = 0
        if page > total_pages:
            page = total_pages
        self.write('<div style="width:100%">')
        if total_pages > 1:
            self.write('<center>%s</center>' % '&nbsp;&nbsp;'.join([i == page and ('%s' %i) or ('<a href=/slice/rep_slices?repo=%s&rep=%s&page=%d>%d</a>' % (repo,(slices[0].units_num), i, i)) for i in range(total_pages)]))

    def _show_slices_for_rep(self,repo ,slices,page):
        self._show_slices_by_page(repo,slices,page)

    def _show_state_info_count(self, repo, slices_and_state_count):
        self.write('<h3 align=center>Slices\' Information in %s</h3>' % repo)
        self.write('<table align=center border=0 cellpadding=8 cellspacing=1 id=cont><tr class=active>')
        for state in Cli.g_slice_states:
            px_len = 10 * len(state)
            self.write('<th width=%dpx nowrap>%s'%(px_len,state))
        self.write('<th width=60px nowrap id=one>1 Reps <th width=60px nowrap id=one>2 Reps')
        self.write('<tr class=normal>')
        for state in Cli.g_slice_states:
            if slices_and_state_count[1][state] == 0:
                self.write('<td align="center" nowrap>0')
            else:
                self.write('<td align="center" nowrap><a href=/slice/state?repo=%s&state=%s>%s</a>' % (repo, state.replace(' ',''), slices_and_state_count[1][state]))
        for slice in slices_and_state_count[0]:
            if slice.units_num == 1 or slice.units_num == 2:
                self.slices_rep[slice.units_num-1].append(slice)
        rep1 = len(self.slices_rep[0])
        rep2 = len(self.slices_rep[1])
        if rep1 == 0:
            self.write('<td align="center" nowrap>%d'%0)
        else:
            self.write('<td align="center" nowrap><a href=/slice/rep_slices?repo=%s&rep=%d>%d</a>'%(repo, 1, rep1))
        if rep2 == 0:
            self.write('<td align="center" nowrap>%d'%0)
        else:
            self.write('<td align="center" nowrap> <a href=/slice/rep_slices?repo=%s&rep=%d>%d</a>'%(repo, 2, rep2))

        self.write('</table></div>')

    def _show_slices_by_page(self, repo,slices,page='0'):
        page= int(page)
        self.write('<h3 align=center>Slices in Repository %s</h3>' % repo)
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=50px nowrap id=slice_id>SliceID
<th width=60px nowrap id=unit>Units_Num
<th width=90px nowrap id=revision>RepRevision
<th width=50px nowrap id=state>State''')
        if len(slices) == 0:
            self.write('</table></div>')
            return
        start = page*200
        if (page+1)*200 > len(slices):
            end = len(slices)
        else:
            end = 200*(page+1)-1
        for i in range(start, end):
            slice = slices[i]
            self.write('<tr class=normal>')
            self.write('<td align="center" nowrap><a href=/slice/slice?repo=%s&slice_no=%d>%d</a>'
                    % (repo, slice.slice_id, slice.slice_id))
            self.write('<td align="center" nowrap>%s' % slices[i].units_num)
            self.write('<td align="center" nowrap>%s' % slices[i].rep_revision)
            self.write('<td align="center" nowrap>%s' % slices[i].state)
        self.write('</table></div>')


    def _show_slices(self, repo, slices, page='0'):
        page= int(page)
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=50px nowrap id=slice_id>SliceID
<th width=60px nowrap id=unit>Units_Num
<th width=90px nowrap id=revision>RepRevision
<th width=50px nowrap id=state>State''')
        if len(slices) == 0:
            self.write('</table></div>')
            return
        for i in range(len(slices)):
            slice = slices[i]
            if slice.slice_id / 1000 == int(page):
                if int(slice.units_num) == 0:
                    self.write('<tr class=error><td>%d' % i)
                else:
                    self.write('<tr class=normal>')
                    self.write('<td align="center" nowrap><a href=/slice/slice?repo=%s&slice_no=%d>%d</a>'
                            % (repo, slice.slice_id, slice.slice_id))
                    self.write('<td align="center" nowrap>%s' % slices[i].units_num)
                self.write('<td align="center" nowrap>%s' % slices[i].rep_revision)
                self.write('<td align="center" nowrap>%s' % slices[i].state)
            elif slice.slice_id / 1000 > int(page):
                break
        self.write('</table></div>')
 
    def state(self, repo, state, page='0'):
        self.print_items(repo, 0)
        self.slices_and_state_count = Cli.list_slice(repo)
        
        if state == 'deletingsnapshot':
            state = 'deleting snapshot'
        elif state == 'creatingsnapshot':
            state = 'creating snapshot'
        self._show_page_index_for_state(repo, state, page)
        self._show_slices_by_page(repo,self.slices_and_state_count[2][state], page)
    def slice(self, repo, slice_no):
        self.slice_infos = Cli.show_slice(repo, slice_no)
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 1)
        items = [['全部slices', '/slice/all?repo=%s' % repo]]
        HTTPRequestHandler.print_items(self, items)
        self.write('<div style="width:100%">')
        self.write('<h2 align=center>Units Info</h2>')
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=50 nowrap id=table_id>TableID
<th width=50 nowrap id=slice_id>SliceID
<th width=130px nowrap id=node>Node
<th width=150px nowrap id=unit_addr>UnitAddr
<th width=150px nowrap id=repl_addr>ReplAddr
<th width=80px nowrap id=rep_order>Rep序
<th width=30px nowrap id=revision>Revision
<th width=30px nowrap id=rep_revision>RepRevision
<th width=30px nowrap id=state>State
<th width=100px nowrap id=ssd_used>SSD Used
<th width=100px nowrap id=disk_used>Disk Used
<th width=40px nowrap id=n_patches>#Patch
<th width=40px nowrap id=n_records>#Rec ''')
        if Settings.has_quota:
            self.write('''<th width=200px nowrap id=quota>Quota''')
        if len(self.slice_infos) == 0:
            self.write('</table></div>')
            return
        units = self.slice_infos[1]
        units.sort(key=lambda x : x.rep_order)
        for i in range(len(units)):
            unit = units[i]
            ip,port = unit.node.split(':')
            ip_split = ip.split('.')
            port = int(port)
            node = int(ip_split[3])<<56|int(ip_split[2])<<48|int(ip_split[1])<<40|int(ip_split[0])<<32|port<<16
            self.write('<tr class=normal>')
            self.write('<td nowrap>%d' % unit.table_id)
            self.write('<td nowrap>%d' % unit.slice_id)
            self.write('<td nowrap><a href="/machine/one?node=%s&machine=%s">%s</a>' 
                    % (node, unit.node, unit.node))
            self.write('<td nowrap>%s' % unit.unit_addr)
            self.write('<td nowrap>%s' % unit.repl_addr)
            self.write('<td nowrap>%d' % unit.rep_order)
            self.write('<td nowrap>%d' % unit.revision)
            self.write('<td nowrap>%d' % unit.rep_revision)
            self.write('<td nowrap>%s' % unit.state)
            self.write('<td nowrap>%f' % unit.ssd_used)
            self.write('<td nowrap>%f' % unit.disk_used)
            self.write('<td nowrap>%d' % unit.nPatches)
            self.write('<td nowrap>%d' % unit.nRecords)
            if Settings.has_quota:
                self.write('<td nowrap>Mem used/Quota(MB):%s<br>Ssd+Disk(GB):%s' % (unit.mem_used_quota, unit.quota_ssd_disk))
        self.write('</table>')
        self.write('<h2 align=center>Snapshots Info</h2>')
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=80 nowrap id=slice_id>SliceID
<th width=100px nowrap id=name>Name
<th width=80 nowrap id=snapshotid>SnapshotID
<th width=100px nowrap id=create_time>CreateTime
<th width=80 nowrap id=life_time>LifeTime
<th width=50 nowrap id=type>Type
<th width=50 nowrap id=s_state>State''')
        snapshots = self.slice_infos[2]
        for i in range(len(snapshots)):
            snapshot = snapshots[i]
            self.write('<tr class=normal>')
            self.write('<td nowrap>%s' % snapshot.slice_id)
            self.write('<td nowrap><a href="/snapshot/one?table=%s&ss=%s">%s</a>' 
                    % (repo, snapshot.name, snapshot.name))
            self.write('<td nowrap>%d' % snapshot.snapshot_id)
            self.write('<td nowrap>%s' % snapshot.create_time)
            self.write('<td nowrap>%d' % snapshot.life_time)
            self.write('<td nowrap>%s' % snapshot.type)
            self.write('<td nowrap>%s' % snapshot.state)
        self.write('</table>')


    def error(self, repo):
        self.print_items(repo, 1)
        self.write('''<div style="width:60%;margin:0 auto">
<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=10% nowrap id=cache>库号
<th width=10% nowrap id=mirror>镜像
<th width=30% nowrap id=host>地址
<th width=15% nowrap id=ip>IP
<th width=10% nowrap id=port>端口
<th width=10% nowrap id=tid>表号
<th width=15% nowrap id=status>状态''')
        repository = mediator.get_repository(repo)
        for i in self.caches:
            cache = self.caches[i]
            m = []
            for j in range(len(cache.mirrors)):
                mirror = cache.mirrors[j]
                if mirror.corrupted:
                    m.append((j, mirror))
            for j in range(len(m)):
                id, mirror = m[j]
                if mirror.corrupted:
                    st = 'error'
                else:
                    continue
                self.write('<tr class=%s>' % st)
                if j == 0:
                    self.write('<td rowspan=%d>%d' % (len(m), i - repository.base_port))
                self.write('<td>%d<td nowrap>%s<td nowrap>%s<td>%d<td>%d<td nowrap>%s' \
                           % (id, mirror.addr.host, mirror.addr.ip, i, mirror.table_id, get_mirror_display(mirror)))
        self.write('</table></div>')

    def machine(self, host, page='1'):
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 1)
        machine = get_machine(host)
        mirrors = []
        self.write('<table align="center"><tr class=normal><td class=active>HOST<td>%s<td class=active>IP<td>%s</table>' %
                   (machine.addr.host, machine.addr.ip))
        if machine.is_type(MachineManager.MachineType.BAILING) or machine.is_type(MachineManager.MachineType.WDND):
            if machine.status and machine.status.wdbd:
                for port, tid in machine.status.wdbd.port_mapping.items():
                    mirror = Mirror(DNS.resolve(machine.addr.ip, port), tid)
                    mirror.set_status(machine.status.wdbd.tables[tid])
                    mirrors.append(mirror)
        self._write_table(mirrors, '/cache/machine?host=%s' % host, page)

    def _write_table(self, inmirrors, urlarg, page='1'):
        page = int(page)
        total_pages = (len(inmirrors) - 1) / 100 + 1
        if page < 1:
            page = 1
        if page > total_pages:
            page = total_pages
        self.write('<div style="width:100%">')
        if total_pages > 1:
            self.write('<center>%s</center>' % '&nbsp;&nbsp;'.join([i + 1 == page and ('%d' %(i + 1)) 
                or ('<a href="%s&page=%d">%d</a>' % (urlarg, i + 1, i + 1)) for i in range(total_pages)]))
        mirrors = inmirrors[(page - 1) * 100 : min(page * 100, len(inmirrors))]
        
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=300px nowrap id=host>地址
<th width=150px nowrap id=ip>IP
<th width=100px nowrap id=port>端口
<th width=10% nowrap id=tid>表号
<th width=15% nowrap id=status>状态
''')
            
        if len(mirrors) == 0:
            self.write('</table></div>')
            return
        for mirror in mirrors:
            if mirror.corrupted:
                st = 'error'
            else:
                st = 'normal'
            self.write('<tr class=%s>' % st)
            self.write(''.join(['<td>%s' % (str) for str in 
                [remove_baidu_tail(mirror.addr.host), mirror.addr.ip, mirror.addr.port, mirror.table_id 
                ]] ))
            self.write('<td nowrap>%s' % get_mirror_display(mirror))
            self.write('</tr>')
        self.write('</table></div>')

class WriteProxyHandler(HTTPRequestHandler):
    def print_items(self, items = None):
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 6)
        items = [['全部(%d)' % len(MachineManager.repo_writeproxys[repo]), '/writeproxy/all?repo=%s' % repo]]
        HTTPRequestHandler.print_items(self, items)

    def all(self, repo = None, page = '1'):
        if (repo is None):
            self.redirect('repository/all')
            return
        self.print_items()
        self.write('<h3 align=center>WriteProxy for Repository %s</h3>' % repo)
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=150px nowrap id=host>host
<th width=150px nowrap id=last_check_time>last check time''')
        self.write('''<th width=220px nowrap id=fifo_dest>fifo dest num(Adjusting)''')
        self.write('''<th width=220px nowrap id=feed_dest>feed dest num(Adjusting)''')
        wps = []
        for wp_ip in  MachineManager.repo_writeproxys[repo]:
            wps.append(MachineManager.writeproxys[wp_ip])
        wps.sort(lambda x,y: cmp(x.last_check_time, y.last_check_time))
        
        for wp in wps:
            self.write('<tr class=normal>' )
            self.write('<td nowrap>%s' % wp.ip)
            self.write('<td nowrap>%s' % time.strftime('%m月%d日 %H:%M:%S ',time.localtime(wp.last_check_time)))
            fifo_d_value = int(len(wp.dests[0])) - wp.valid_dest_count[0]
            self.write('<td nowrap>%d(%d)' % (int(len(wp.dests[0])), fifo_d_value))
            feed_d_value = int(len(wp.dests[1])) - wp.valid_dest_count[1]
            self.write('<td nowrap>%d(%d)' % (int(len(wp.dests[1])), feed_d_value))
        self.write('</table>')

    def writeproxy(self,machine):
        return
    
    def fifo_dests(self,dests):
        return

    def feed_dests(self,feeds):
        return

class SnapshotHandler(HTTPRequestHandler):
    def print_items(self, items = None):
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 2)
        HTTPRequestHandler.print_items(self, items)
        
    def all(self, repo = None, page = '1'):
        if (repo is None):
            self.redirect('/repository/all')
            return
        snapshots = Cli.list_snapshot(repo)
        self.print_items()
        self.write('<h3 align=center>Snapshots in Repository %s</h3>' % repo)
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=100px nowrap id=name>Name
<th width=80px nowrap id=normal>Normal
<th width=80px nowrap id=creating>Creating
<th width=80px nowrap id=deleting>Deleting
''')
        for ss in snapshots:
            self.write('<tr class=normal>')
            self.write('<td nowrap><a href="/snapshot/one?table=%s&ss=%s">%s</a>' % (repo, ss[0], ss[0]))
            self.write('<td nowrap>%d' % int(ss[1]))
            self.write('<td nowrap>%d' % int(ss[2]))
            self.write('<td nowrap>%d' % int(ss[3]))
        self.write('</table>')
    
    def one(self, table, ss, page='0'):
        self.print_items()
        ss_info = Cli.show_snapshot(table, ss)
        self._write_table(ss_info, '/snapshot/one?table=%s&ss=%s' % (table, ss), table, page)
    
    def _write_table(self, ss_info, urlarg, table, page='0'):
        page = int(page)
        if len(ss_info) > 0:
            total_pages = int(ss_info[len(ss_info) - 1][0]) / 1000 + 1
        valid_pages = [0] * total_pages
        for ss in ss_info:
            valid_pages[int(ss[0]) / 1000] = 1
        valid_pages = [n for n, v in enumerate(valid_pages) if v == 1]
        if page <= 0:
            page = valid_pages[0]
        if page > total_pages:
            page = total_pages[-1]
        self.write('<div style="width:100%">')
        if total_pages > 1:
            self.write('<center>%s</center>' % '&nbsp;&nbsp;'.join([i == page and ('%s' % i)
                or ('<a href="%s&page=%d">%d</a>' % (urlarg, i, i)) for i in valid_pages]))
        
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=50px nowrap id=sliceid>SliceID
<th width=150px nowrap id=name>Name
<th width=50px nowrap id=snapshotID>SnapshotID
<th width=120px nowrap id=create_time>CreateTime
<th width=120px nowrap id=life_time>LifeTime
<th width=60px nowrap id=type>Type
<th width=80px nowrap id=state>State
''')
        
        if len(ss_info) == 0:
            self.write('</table></div>')
            return
        for ss in ss_info:
            if int(ss[0]) / 1000 == int(page):
                ss[0] = '<a href="/slice/slice?repo=%s&slice_no=%s">%s</a>' % (table, ss[0].strip(), ss[0])
                rowspan = 'rowspan=1'
                self.write('<tr class=normal>')
                self.write(''.join(['<td %s>%s' % (rowspan, str.strip()) for str in ss]))
                self.write('</tr>')
            elif int(ss[0]) / 1000 > int(page):
                break
        self.write('</table></div>')

TOTAL_SIGN = 'Total: '
TOTAL_DIFF_SIGN = 'Totaldiff: '
def generate_record_count_webpage(repository):
    ret = ''
    ret += '''<div style="width:100%;margin:0 auto">
<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=50px nowrap id=no>no
<th width=100px nowrap id=mirror1>mirror1
<th width=150px nowrap id=mirror2>mirror2
<th width=150px nowrap id=mirror3>mirror3
<th width=200px nowrap id=mirror_other>other mirrors
'''
    caches_map = repository.get_caches_map()
    tot = 0
    tot_diff = 0
    for port in range(repository.base_port, repository.base_port + repository.cache_num):
        ret += '<tr class=normal>'
        ret += '<td>%d' % (port - repository.base_port)
        if port in caches_map:
            mirror1_count = -1
            first = True
            for mirror in caches_map[port].mirrors:
                if mirror.status:
                    c = mirror.status.record_count
                else:
                    c = 0
                if first:
                    mirror1_count = c
                    ret += '<td>%d' % c
                    tot += c
                    first = False
                else:
                    ret += '<td>%d (%d)' % (c, c - mirror1_count)
                    tot_diff += abs(c - mirror1_count)
    ret += '<tr class=active><td>Tot<td>%d<td>Tot-diff<td>%d' % (tot, tot_diff)
    ret += '</table>\n'
    ret += '%s%d\n%s%d\n' % (TOTAL_SIGN, tot, TOTAL_DIFF_SIGN, tot_diff)
    return ret

class RepositoryHandler(HTTPRequestHandler):
    def print_items(self, active):
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 4)
        items = [['all', '/repository/all'], \
                 ]
        items[active][1] = ''
        HTTPRequestHandler.print_items(self, items)
        
    def all(self):
        self.print_items(0)
        try:
            repositories = Cli.list_table()
        except:
            common.print_exc_plus()
        str_th = '''<div style="width:100%;margin:0 auto">
<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>  
<th width=80px nowrap id=name>Name
<th width=30px   id=tid>Id
<th width=50px   id=partitions>Partitions
<th width=50px   id=slice_num>SliceN
<th width=30px   id=replica_num>Replica
<th width=80  id=status>Status
<th width=50  id=revision>Revision
<th width=80px   id=slices>Slices
<th width=80px   id=snapshots>Snapshots
<th width=80px   id=snapshots>WriteProxy'''
        if Settings.has_quota:
            str_th+='''<th width=150px id=mem_quota>Mem Quota(MB)
<th width=150px id=disk_quota>Disk Quota(GB)
<th width=150px id=bailing_mode>Bailing Mode'''
        str_th += (self.check_authorization(False) and '''
<th width=80px  id=rd_token>rd token
<th width=80px  id=rw_token>rw token''' or '')
        self.write(str_th)
        for repo in sorted(repositories, key = lambda x : x.name):
            rowspan = 'rowspan=1'
            self.write('<tr class=normal>')
            str_td = ''.join(['<td %s><center>%s</center>' % (rowspan, str) for str in 
                [repo.name, repo.table_id, repo.partitions, repo.slice_num, repo.replica_num, repo.state, repo.revision
                , '<a href="/slice/all?repo=%s">slices</a>' % repo.name
                , '<a href="/snapshot/all?repo=%s">snapshots</a>' % repo.name
                , '<a href="/writeproxy/all?repo=%s">writeproxy</a>' % repo.name]])
            if Settings.has_quota :
                for str in[repo.mem_quota,repo.disk_quota,repo.bailing_mode]:
                    str_td += '<td %s><center>%s</center>' % (rowspan, str)
            str_td.join((self.check_authorization(False) and [repo.rd_token] or []) + 
                    (self.check_authorization(False) and [repo.rw_token] or []) )
            self.write(str_td)
            self.write('</tr>')
        self.write('</table>')
    
    def record_count(self, repo):
        repository = get_repository(repo)
        if repository is None:
            self.redirect('/repository/all')
            return
        self.print_items(0)
        self.write(generate_record_count_webpage(repository))
        
    def get_merge_status(self, repo, tag):
        repository = get_repository(repo)
        if repository is None:
            self.write('ERR_NO_REPO')
            return
        if tag not in repository.merge_requests:
            self.write("ERR_NO_TAG")
            return
        self.write("%s" % repository.merge_requests[tag].status)

    def _generate_job_table(self, jobs, job_status = None):
        op_operation = False
        if self.read_cookie('passcode') == Settings.op_passcode:
            op_operation = True
        self.write('''<div style="width:100%;margin:0 auto">
<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=150px nowrap id=job_name>job name
<th width=200px nowrap id=task_name>task name
<th width=180px nowrap id=task_start_time>start time
<th width=180px nowrap id=task_start_time>end time
<th width=150px nowrap id=task_id>task id
<th width=100px nowrap id=machine>machine
<th width=50px nowrap id=status>status
''')
        if op_operation:
            self.write('''<th width=50px nowrap id=ops>logs ''')
        for job in jobs:
            first = True
            for task in job.tasks:
                if task.is_fail():
                    col = 'error'
                elif task.is_succ() or task.is_pend():
                    col = 'normal'
                else:
                    col = 'warning'
                self.write('<tr class=%s>' % col)
                if first:
                    first = False
                    self.write('<td rowspan=%s>%s' % (len(job.tasks), job.name))
                self.write('<td>%s<td>%s<td>%s<td>%s<td>%s<td>%s' % (task.name, format_time(task.start_time), format_time(task.end_time), task.run_taskid, task.get_run_machine(), task.status))
                if op_operation:
                    self.write('<td><a href=get_task_log?job_name=%s&task_name=%s> log </a></td>' 
                               % (job.name, task.name))
        self.write('</table>')
    
    def gen_pages(self, objects, href, page, call_back, other_arguments):
        self.write(' Pages: ')
        self.write('<a href=%s&page=0> first page </a>' % href)
        length = len(objects)
        page_num = (length - 1) / 50 +1
        page_start = int(page / 50) * 50
        if page_start >= 50:
            self.write('<a href=%s&page=%s> << </a>' % (href, page_start - 50))
        page_end = page_start + 50
        if page_num < page_end:
            page_end = page_num
        for i in range(page_start, page_end):
            self.write('<a href=%s&page=%s>%s</a> ' % (href, i, i))
        if page_end < page_num:
            self.write('<a href=%s&page=%s> >> </a>' % (href, page_end))
        if page_num < 1:
            page_num = 1
        self.write('<a href=%s&page=%s> last page </a>' % (href, page_num - 1))
        self.write('<br>')
        start = page * 50
        end = (page + 1) * 50
        call_back(objects[start : end], other_arguments)
        
    def merge_status(self, repo = None):
        repository = get_repository(repo)
        if repository is None:
            self.redirect('/repository/all')
            return
        self.print_items(0)
        self.write('''<div style="width:100%;margin:0 auto">
<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=200px nowrap id=tag>tag
<th width=200px nowrap id=trigger_time>trigger time
<th width=200px nowrap id=end_time>end time
<th width=150px nowrap id=done_unhold_fifo>done unhold fifo
<th width=100px nowrap id=status>status
<th width=150px nowrap id=jobs>jobs
<th width=150px nowrap id=ops>ops
''')
        for status in sorted(repository.merge_requests.values(), key = lambda x : x.trigger_time, reverse = True)[0:50]:
            self.write('<tr class=normal>')
            self.write('<td>%s' % (status.tag))
            self.write('<td>%s' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(status.trigger_time))))
            self.write('<td>%s' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(status.end_time))))
            self.write('<td>%s' % (status.done_unhold_fifo))
            self.write('<td>%s' % (status.status))
            self.write('<td><a href=kill_merge_request?repo=%s&tag=%s>kill</a> <a href=resume_merge_request?repo=%s&tag=%s>resume</a>' 
                       % (repo, status.tag, repo, status.tag))
            self.write('</tr>')
        self.write('</table>')
        
    def fix_status(self, repo = None):
        repository = get_repository(repo)
        if repository is None:
            self.redirect('/repository/all')
            return
        self.print_items(0)
        self.write('''<div style="width:100%;margin:0 auto">
<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=200px nowrap id=tag>fix tag
<th width=300px nowrap id=trigger_time>trigger time
<th width=300px nowrap id=end_time>end time
<th width=150px nowrap id=done_unhold_fifo>done unhold fifo
<th width=100px nowrap id=status>status
<th width=100px nowrap id=jobs>jobs
<th width=150px nowrap id=ops>ops
''')
        for fix_request in sorted(repository.fix_requests.values(), key = lambda x : x.trigger_time, reverse = True)[0:50]:
            self.write('<tr class=normal>')
            self.write('<td>%s' % (fix_request.tag))
            self.write('<td>%s' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(fix_request.trigger_time))))
            self.write('<td>%s' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(fix_request.end_time))))
            self.write('<td>%s' % (fix_request.done_unhold_fifo))
            self.write('<td>%s' % (fix_request.status))
            self.write('<td><a href=kill_fix_request?repo=%s&tag=%s>kill</a> <a href=resume_fix_request?repo=%s&tag=%s>resume</a>' 
                       % (repo, fix_request.tag, repo, fix_request.tag))
        self.write('</table>')

    def set_comments(self, repo_name, comments, emails):
        if not self.check_authorization():
            return
        self.disable_cache()
        comments = comments.replace(' ', '_').replace('\n', '|')
        ret = msg = ''
        try:
            if emails != 'None':
                ret = check_comments_emails(comments, emails)
            if ret == '':
                mediator.set_comments(repo_name, comments, emails)
            else:
                msg += '[%s] : %s\n' % (emails, ret)
        except:
            self.write(generate_msg_page('/machine/bailing', traceback.format_exc()))
            traceback.print_exc()
            return
        if msg != '':
            self.write(generate_msg_page('/machine/bailing', msg))
        else:
            self.redirect('/machine/bailing')
       
    def merge(self, repo_name, token, merge_tag, done_unhold_fifo = True, trigger_time = None):
        try:
            if trigger_time is None or not trigger_time.isdigit():
                trigger_time = int(time.time())
            mediator.merge_repository(repo_name, token, merge_tag, trigger_time, bool(int(done_unhold_fifo)))
            self.redirect('/repository/all')
        except:
            self.write(generate_msg_page('/repository/all', traceback.format_exc()))
            traceback.print_exc()
            return
        
    def set_merge_token(self, repo_name, token):
        if not self.check_authorization():
            return
        try:
            mediator.set_merge_token(repo_name, token)
            self.redirect('/repository/all')
        except:
            self.write(generate_msg_page('/repository/all', traceback.format_exc()))
            traceback.print_exc()
            return

    def set_replica_num(self, repo_name, replica_num):
        if not self.check_authorization():
            return
        try:
            mediator.set_replica_num(repo_name, replica_num)
            self.redirect('/repository/all')
        except:
            self.write(generate_msg_page('/repository/all', traceback.format_exc()))
            traceback.print_exc()
            return
        
    
def generate_msg_page(tourl, msg, delay_sec = 300):
    return '''<head><meta http-equiv="refresh" content="%d;url=%s"> </head>
<body><h2>Message</h2><small>redirect to <a href=%s>%s</a> in %d second</small>
<div style="background-color:lightblue;"><pre>%s</pre></div>
</body>''' % (delay_sec, tourl, tourl, tourl, delay_sec, msg)


def generate_form(action_name, action, arg_list):
    html = '<div style="padding: 20px 30px 50px 20px; float: left;"> <b>%s</b><br><br><form action="%s" method="post">' % (action_name, action) 
    for i in xrange(len(arg_list)):
        if type(arg_list[i]) == type(''):
            html += '<label>%s</label><br><input type="text" name="%s"/><br>' % (arg_list[i].split(':')[1], arg_list[i].split(':')[0])
        elif type(arg_list[i]) == type([]):
            if arg_list[i][1] == 'select':
                html += '<label>%s</label><br><select name="%s">' % (arg_list[i][0].split(':')[1], arg_list[i][0].split(':')[0]) 
                for j in xrange(2, len(arg_list[i])):
                    html += '<option value="%s">%s</option>' % (arg_list[i][j].split(':')[0], arg_list[i][j].split(':')[1])
                html += '</select><br>'
            elif arg_list[i][1] == 'textarea':
                html += '<label>%s</label><br><textarea rows="10" cols="30" name="%s"></textarea><br>' % (arg_list[i][0].split(':')[1], arg_list[i][0].split(':')[0])
    html += '<input type="submit" value="Submit"/><br></form></div>'
    return html

def get_repository_name_select(arg_name='name'):
    ret = ['%s:repository name' % arg_name, 'select']
    for repo in sorted(tables_info.values(), key = lambda repo : repo.name):
        ret.append('%s:%s' % (repo.name, repo.name))
    return ret

class OpHandler(HTTPRequestHandler):
    def print_items(self, active):
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 7)
        items = [['all', '/repository/all']]
        items[active][1] = ''
        HTTPRequestHandler.print_items(self, items)

        self.write('''<div style="position: relative; float: left; padding: 20px 30px 50px 20px;" class="active">
<li><a href="/op/login">login</a></li>
<li><a href="/op/apply_repository">申请库种</a></li>
<li><a href="/op/contact">联系我们</a></li>
<hr width=100 align=left>''')
        if not self.check_authorization(False):
            self.write('''<li><a href="/op/merge_repository">merge repository</a></li>''')
        else:
            self.write('''<b>repository</b>
<li><a href="/op/set_replica_num">set_replica_num</a></li>
<li><a href="/op/set_comments">set_comments</a></li>
<hr width=100 align=left>
<b>fifo_machine</b>
<li><a href="/op/add_machine">add</a></li>
<li><a href="/op/remove_machine">remove</a></li>
<li><a href="/op/add_machine_tag">add tag</a></li>
<li><a href="/op/remove_machine_tag">remove tag</a></li>
<hr width=100 align=left>
<b>repository fifo</b>
<li><a href="/op/deploy_fifo">deploy fifo</a></li>
<li><a href="/op/add_fifo_group">add fifo group</a></li>
<li><a href="/op/remove_fifo_group">remove fifo group</a></li>
<li><a href="/op/op_fifo_group">op_fifo_group</a></li>
<li><a href="/repository/get_fifo_no_cache">get_fifo_no_cache</a></li>
<hr width=100 align=left>
<b>merge</b>
<li><a href="/op/merge_repository">merge repository</a></li>
<li><a href="/op/set_merge_token">set_merge_token</a></li>
<li><a href="/op/set_merge_interval">set_merge_interval</a></li>
<hr width=100 align=left>
<b>job&task</b>
<li><a href="/op/reset_task_count">reset_task_count</a></li>
<li><a href="/op/run_count_per_host">run_count_per_host</a></li>
<hr width=100 align=left>
<b>setting</b>
<li><a href="/op/reload_setting">reload setting</a></li>
''')
        self.write('</div> ')
    def all(self):
        self.print_items(0)

    def login(self, passcode = None):
        if passcode is None:
            self.print_items(0)
            self.write(generate_form('login', '/op/login', ['passcode:passcode']))
        elif passcode == 'w':
            self.write('passcode is %s' % self.read_cookie('passcode'))
        else:
            c = Cookie.SimpleCookie()
            c['passcode'] = passcode
            c['passcode']['path'] = '/'
            c['passcode']['expires'] = time.strftime("%a, %d-%b-%Y %H:%M:%S GMT", time.gmtime(time.time() + 20 * 60))
            self.set_header('Set-Cookie', c.output(header=''))
            self.write(generate_msg_page('/op/all', 'login ok'))
            
    def apply_repository(self, name=None, usage=None, page_num=None, page_avg_size=None, replica_num=None, use_filter=None, fifo_group_num=None, need_trigger_merge=None, owner_emails=None):
        arg_name = ['name:库种名(长度范围[1,16])'
                    , 'usage:用途(调研库?线上库?使用时间?)'
                    , 'page_num:网页总数'
                    , 'page_avg_size:平均网页大小(大库为10k)'
                    , ['replica_num:镜像数(一般为3)','select','1:1','2:2','3:3']
                    , ['fifo_group_num:fifo组个数','select','0:0','1:1','2:2','3:3','4:4']
                    , ['need_trigger_merge:是否需要外部触发merge','select','0:No','1:Yes']
                    , 'owner_emails:负责人邮件列表(空格分隔)']
        if name is None:
            self.print_items(0)
            self.write(generate_form('申请库种', '/op/apply_repository', arg_name))
        elif name is None or usage is None or page_num is None or page_avg_size is None or replica_num is None or use_filter is None or fifo_group_num is None \
                or need_trigger_merge is None or owner_emails is None:
            self.write('form not complete.')
        else:
            owner_list = owner_emails.replace(',', ' ').replace(';', ' ').split(' ')
            owner_list = [email for email in owner_list if len(email) > 0]
            sendmail(Settings.emails.split(',') + owner_list, 'Bailing库种申请', \
                     ''.join([(type(desc)==type('') and desc or desc[0]) + ' : %s\n' for desc in arg_name]) % \
                     (name, usage, page_num, page_avg_size, replica_num, use_filter, fifo_group_num, need_trigger_merge, owner_emails))
            self.write(generate_msg_page('/op/all', 'apply email sent, please wait for bailing OP\'s reply.'))

    def set_comments(self):
        self.print_items(0)
        self.write(generate_form('set comments', '/repository/set_comments', 
            [get_repository_name_select('repo_name'), ['comments:Comments on the repository(空格会转为_, 回车会变为|)','textarea'],
             'emails:E-mails(separated by ,)||can be set to "None"']))

    def deploy_fifo(self):
        self.print_items(0)
        self.write(generate_form('deploy_fifo for repository', '/repository/deploy_fifo', [get_repository_name_select('repo_name')]))

    def merge_repository(self):
        self.print_items(0)
        self.write(generate_form('merge_repository for repository', '/repository/merge', 
                                 [get_repository_name_select('repo_name'), 'token:merge token', 'merge_tag:tag (identify the merge)',
                                  ['done_unhold_fifo:unhold fifo after done merge', 'select', '1:Unhold after done', '0:DONT unhold after done'],
                                  'trigger_time:trigger time (optional, default to be current time %d)' % int(time.time())
                                  ]))

    def set_merge_token(self):
        self.print_items(0)
        self.write(generate_form('set_merge_token for repository', '/repository/set_merge_token', 
                                 [get_repository_name_select('repo_name'), 'token:merge token']))

    def set_merge_interval(self):
        self.print_items(0)
        self.write(generate_form('set_merge_interval for repository (minutes)', '/repository/set_merge_interval', 
                                 [get_repository_name_select('repo_name'), 'interval:interval (in minutes)']))

    def set_replica_num(self):
        self.print_items(0)
        self.write(generate_form('set_replica_num for repository', '/repository/set_replica_num', 
                                 [get_repository_name_select('repo_name'), ['replica_num:replica number (normally as 3)','select',
                                 '1:1','2:2','3:3']]))

    def reload_setting(self):
        if not self.check_authorization():
            return
        reload(Settings)
        common.set_logger_level(Settings.log_level)
        self.write('reload ok')
        
    def set_log_level(self, log_level):
        if not self.check_authorization():
            return
        try:
            common.set_logger_level(log_level)
        finally:
            self.write('set done')
        
    def contact(self):
        self.print_items(0)
        self.write('<b>管理员</b><br><br>')
        self.write('<br>'.join(Settings.emails.split(',')))

class FifoHandler(HTTPRequestHandler):
    def print_items(self, repo, active):
        self.slices_and_state_count = Cli.list_slice(repo)
        self.slices = slices_and_state_count[0]
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 1)
        items = [['全部(%d)' % len(self.slices), '/slice/all?repo=%s' % repo]]
        items[active][1] = ''
        HTTPRequestHandler.print_items(self, items)

    def all(self, repo = None, page = '1'):
        if (repo is None):
            self.redirect('/repository/all')
            return
#        fifos = get_fifos(repo)
#        fifos.sort(cmp = lambda x, y : cmp(x.addr.port, y.addr.port))
#        self.print_items()
#        self._write_table(fifos, '/fifo/all?repo=%s' % repo, page)

class UfeedHandler(HTTPRequestHandler):
    def print_items(self, active):
        HTTPRequestHandler.print_header(self)
        HTTPRequestHandler.print_tab(self, 5)
        items = [['all', '/ufeed/all']]
        items[active][1] = ''
        HTTPRequestHandler.print_items(self, items)

        self.write('''<div style="position: relative; float: left; padding: 20px 30px 50px 20px;" class="active">
<li><a href="/ufeed/login">login</a></li>
<li><a href="/ufeed/task_submit">提交任务</a></li>
<li><a href="/ufeed/task_status">查看状态</a></li>
<li><a href="/ufeed/contact">联系我们</a></li>
<hr width=100 align=left>''')
        if not self.check_authorization(False):
            self.write('''<li><a href="/ufeed/other_op">其他操作</a></li>''')
        else:
            self.write('''<b>OP</b>
<li><a href="/ufeed/attr_op">属性操作</a></li>
<li><a href="/ufeed/check_feed">单次回灌检查</a></li>
<li><a href="/ufeed/auto_check_feed">例行回灌检查</a></li>
''')
        self.write('</div> ')

    def all(self):
        self.print_items(0)

    def login(self, passcode = None):
        if passcode is None:
            self.print_items(0)
            self.write(generate_form('login', '/ufeed/login', ['passcode:passcode']))
        elif passcode == 'w':
            self.write('passcode is %s' % self.read_cookie('passcode'))
        else:
            c = Cookie.SimpleCookie()
            c['passcode'] = passcode
            c['passcode']['path'] = '/'
            c['passcode']['expires'] = time.strftime("%a, %d-%b-%Y %H:%M:%S GMT", time.gmtime(time.time() + 20 * 60))
            self.set_header('Set-Cookie', c.output(header=''))
            self.write(generate_msg_page('/ufeed/all', 'login ok'))
#            self.redirect('/ufeed/all')

    def task_submit(self, mod_name=None, attr_name=None, target_name=None, cache_divide=None, diff_mode=None, record_sep=None, \
            data_is=None, feed_data=None, alarm_emails=None, others=None):
        arg_name1 = [ 'mod_name:mod_name 回灌字段所属模块名'
                    , 'attr_name:attr_name 回灌字段名'
                    , 'target_name:target 库种名(长度范围[1,16])'
                    , ['cache_divide:cache-divide 是否使用mapreduce进行分环','select','0:No','1:Yes']
                    , ['diff_mode:diff-mode 是否启用diff-mode模式(diff模式会自动将"库内该字段非默认值"且"不在本次回灌列表"的页面清为默认值)','select','0:No','1:Yes']
                    , ['record_sep:record-sep 回灌数据中的记录分隔符','select','1:\\n','0:\\0','2:binary']
                    , 'alarm_emails:alarm-emails 报警邮箱(逗号分隔)'
                    , ['data_is:回灌数据上传方式','select','1:ftp','2:hdfs_source']
                    , 'feed_data:回灌数据路径(ftp方式给出数据的目录,hdfs_source方式给出hdfs_source文件的绝对路径)'
                    , ['others:其他参数配置(一行一个,详见<a href="http://wiki.babel.baidu.com/twiki/bin/view/Ps/Inf/UnifiedFeedManual">wiki</a>)','textarea']]
#        arg_name2 = ['feed_value:(是否回灌统一的值.回灌任意值时忽略该行配置)'
#                    , 'url_col:url在数据文件中的列号,列号从1开始计数(仅在record分隔符为\\n时生效).当使用cache-divide时, url-col只能为1'
#                    , 'value_col:回灌数据在数据文件中的列号,列号从1开始计数(仅在record分隔符为\\n时生效).当使用feed-value时,同时需要url-col时,value-col选项必须设置为0'
#                    , 'feed_dest:回灌目标数据管道(fifo),如挖掘wdnd灌入大库,需要灌入数据通道,而非回灌通道,需要配此选项(具体值需要与op确认)'
#                    , 'check_attr:mod_if模式下检查的条件属性'
#                    , 'check_col:mod_if模式下条件属性值的列号,列号从1开始计数(仅在record分隔符为\\n时生效).不指定该值时默认第1列为url,第2列为回灌值,第3列为条件属性值'
#                    , ['no_histdata:是否需要在集群间拷贝回灌数据并保留,默认是No','select','0:No','1:Yes']
#                    , ['no_autodivide:是否需要自动判断分环,默认是No','select','0:No','1:Yes']
#                    , 'join_target:针对指定库种,先求回灌页面与库内页面的交集再回灌'
#                    , 'm_feed_attr:多字段回灌模式下的回灌属性名(空格分隔)'
#                    , 'm_url_col:多字段回灌模式下url在数据文件中的列号,列号从1开始计数(仅在record分隔符为\\n时生效).当使用cache-divide时,url-col只能为1'
#                    , 'm_value_col:多字段回灌模式下回灌数据在数据文件中的列号,列号从1开始计数(仅在record分隔符为\\n时生效)'
#                    , 'm_feed_value:多字段回灌模式下是否回灌统一的值,如果需要回灌任意值,忽略掉该行配置']
        if mod_name is None or attr_name is None:
            self.print_items(0)
            self.write(Ufeed.generate_form('回灌参数', '/ufeed/task_submit', arg_name1))
#            self.write(generate_form('可选参数', '/ufeed/task_submit', arg_name2))
        elif target_name is None or diff_mode is None or cache_divide is None or record_sep is None or data_is is None or feed_data is None :
            self.write(generate_msg_page('/ufeed/all', 'form not complete.'))
#            self.write('form not complete.')
        else:
            how = {'1':'-D', '2':'-F'}
            if alarm_emails != None:
                if alarm_emails.find(Ufeed.ufeed_emails) != -1:
                    alarm_emails = alarm_emails+','+Ufeed.ufeed_emails
            else:
                alarm_emails = Ufeed.ufeed_emails
            cmd = 'sh %s/submit_task.sh %s %s %s %s "target:%s" "cache-divide:%s" "diff-mode:%s" "record-sep:%s" "alarm-email:%s"' % (Ufeed.ufeed_dir, mod_name, attr_name, how[data_is], feed_data, target_name, cache_divide, diff_mode, record_sep, alarm_emails)

            others = others.split('\r\n')
            for other in others:
                cmd = cmd + ' "%s"' % other
            print cmd 
            shell_output = Ufeed.run_shell(cmd)
            
            self.write(generate_msg_page('/ufeed/all', shell_output))
            
    def task_status(self):
        self.print_items(0)

        self.write('<h2 align="center">回灌任务状态</h2>')
        
        tasks = []
        shell_output = Ufeed.run_shell('sh %s/check_feed_status.sh' % Ufeed.ufeed_dir).split("\n")[2:]
        for each in shell_output:
            task = each.split("\t")
            if len(task) == 5:
                tasks.append(Ufeed.Task(task[0], task[1], task[2], task[3], task[4]))
        self._write_table(tasks,'/ufeed/task_status')
        
    def _write_table(self, intasks, urlarg, page='1'):
        page = int(page)
        total_pages = (len(intasks) - 1) / 100 + 1
        if page < 1:
            page = 1
        if page > total_pages:
            page = total_pages
        self.write('<div style="width:100%">')
        if total_pages > 1:
            self.write('<center>%s</center>' % '&nbsp;&nbsp;'.join([i + 1 == page and ('%d' %(i + 1)) 
                or ('<a href="%s&page=%d">%d</a>' % (urlarg, i + 1, i + 1)) for i in range(total_pages)]))
        tasks = intasks[(page - 1) * 100 : min(page * 100, len(intasks))]
        
        self.write('''<table align=center border=0 cellpadding=8 cellspacing=1 id=cont>
<tr class=active>
<th width=200px nowrap id=uptime>任务名称
<th width=200px nowrap id=uptime>提交时间
<th width=200px nowrap id=checktime>check时间
<th width=200px nowrap id=cachedivtime>分环时间
<th width=200px nowrap id=feedtime>回灌时间
''')
            
        if len(tasks) == 0:
            self.write('</table></div>')
            return
        for task in tasks:
            self.write('<tr class=normal>')
            self.write(''.join(['<td>%s' % (str) for str in 
                [task.name, task.uptime, task.checktime, task.cachedivtime, task.feedtime 
                ]] ))
#            self.write('<td nowrap>')
            self.write('</tr>')
        self.write('</table></div>')

    def attr_op(self, op=None, mod_name=None, attr_name=None, arg_dump=None,
            arg_row=None, arg_p=None, arg_u=None, is_seek=None, attr_def=0,
            attr_def_feed=0, arg_seek='TITL', arg_rs=None, auto_addftp=None):
        arg_name = [ ['op:操作(ADD|MOD|DEL|SEEK)','select','0:ADD','1:MOD','2:DEL','3:SEEK']
                    , 'attr_name:attr_name 操作的回灌属性字段名attr'
                    , 'mod_name:mod_name 该字段对应的回灌模块名mod'
                    , 'arg_dump:arg_dump 通过bailing_stat.sh获取该字段值的参数名'
                    , 'arg_row:arg_row 该字段通过seeksign工具中获取的ul_pack包显示的字段名'
                    , ['arg_p:是否使用MOD_P方式回灌,默认是Yes','select','1:Yes','0:No']
                    , ['arg_u:回灌是否更新intime字段,默认是No','select','0:No','1:Yes']
                    , ['is_seek:is_seek 检查回灌效果是否需要通过seeksign确认,默认是Yes','select','1:Yes','0:No']
                    , 'attr_def:attr_def 该属性字段的默认值或其长度值,默认是0'
                    , 'attr_def_feed:attr_def_feed 该属性字段在库中的默认值,默认是0'
                    , 'arg_seek:arg_seek 该字段通过seeksign工具获取信息时需要的data_ids,默认是TITL'
                    , ['arg_rs:arg_rs 该属性字段对应的记录分隔符,默认是\\n','select','1:\\n','0:\\0','2:binary']
                    , ['auto_addftp:是否自动添加对应的ftp账号,默认是No','select','0:No','1:Yes']]
        if op is None:
            self.print_items(0)
            self.write(generate_form('属性操作', '/ufeed/attr_op', arg_name))
        elif attr_name is None:
                self.write(generate_msg_page('/ufeed/all', 'attr_name needed.'))
#                self.write('form not complete.')
        else:
            run = 1
            ops = {'0':'ADD', '1':'MOD', '2':'DEL', '3':'SEEK'}
            if op == '0' or op == '1':
                if mod_name is None or arg_dump is None or arg_row is None:
                    self.write(generate_msg_page('/ufeed/all', 'form not complete.'))
                    run = 0
                else:
                    cmd = 'sh %s/attr_tools.sh %s -m %s -a %s -d %s -r %s -p %s -u %s -s %s -f %s -F %s -S "%s" -R %s -t %s' % (Ufeed.ufeed_dir, ops[op], mod_name, attr_name, arg_dump, arg_row, arg_p, arg_u, is_seek, attr_def, attr_def_feed, arg_seek, arg_rs, auto_addftp)
            elif op == '2':
                cmd = 'sh %s/attr_tools.sh %s -a %s' % (Ufeed.ufeed_dir, ops[op], attr_name)
            else:
                cmd = 'sh %s/get_arg.sh %s ALL' % (Ufeed.ufeed_dir, attr_name)
            
            if run == 1:
                shell_output = Ufeed.run_shell(cmd)
                self.write(generate_msg_page('/ufeed/all', shell_output))

    def check_feed(self, check_tasks=None, check_emails=None):
        arg_name = [['check_tasks:任务列表(每行一个)','textarea'],
             'check_emails:E-mails(逗号分隔,可以设置为None)']
        if check_tasks is None:
            self.print_items(0)
            self.write(generate_form('单次回灌任务检查', '/ufeed/check_feed', arg_name))
        else:
            cmd = 'sh %s/check_feed_result.sh' % (Ufeed.ufeed_dir)
            if check_emails is not None:
                cmd = cmd + ' -e %s' % check_emails
            check_tasks = check_tasks.split('\r\n')
            for task in check_tasks:
                cmd = cmd + ' %s' % task
            print cmd
            shell_output = Ufeed.run_shell(cmd)
            self.write(generate_msg_page('/ufeed/all', shell_output))

    def _add_check_list(self, add_check_dict):
        dict = {}
        if os.path.exists(Ufeed.ufeed_dir + Ufeed.ufeed_check_file):
            f = open(Ufeed.ufeed_dir + Ufeed.ufeed_check_file, 'r')
            for eachline in f.readlines():
                if eachline != '':
                    dict[eachline.split(' '*2)[0]] = eachline.split(' '*2)[1].split('\n')[0]
            f.close()
        for k in add_check_dict.keys():
            if k in dict.keys():
                if add_check_dict[k] is not None:
                    if dict[k] != 'None':
                        dict[k] = add_check_dict[k] + ',' + dict[k]
                    else:
                        dict[k] = add_check_dict[k]
            else:
                dict[k] = add_check_dict[k]

        output = 'ATTR\tEMAILS\n'
        f = open(Ufeed.ufeed_dir + Ufeed.ufeed_check_file, 'w')
        for k in dict.keys():
            if dict[k] is None:
                dict[k] = 'None'
            output += '%s\t%s\n' % (k, dict[k])
            f.write('%s  %s\n' % (k, dict[k]))
        f.close()
        self.write(generate_msg_page('/ufeed/all', output))

    def _del_check_list(self, del_check_list):
        dict = {}
        if os.path.exists(Ufeed.ufeed_dir + Ufeed.ufeed_check_file):
            f = open(Ufeed.ufeed_dir + Ufeed.ufeed_check_file, 'r')
            for eachline in f.readlines():
                if eachline != '':
                    dict[eachline.split(' '*2)[0]] = eachline.split(' '*2)[1].split('\n')[0]
            f.close()
        for attr in del_check_list:
            attr = attr.strip()
            if attr in dict.keys():
                del dict[attr]

        output = 'ATTR\tEMAILS\n'
        f = open(Ufeed.ufeed_dir + Ufeed.ufeed_check_file, 'w')
        for k in dict.keys():
            if dict[k] is None:
                dict[k] = 'None'
            output += '%s\t%s\n' % (k, dict[k])
            f.write('%s  %s\n' % (k, dict[k]))
        f.close()
        self.write(generate_msg_page('/ufeed/all', output))
            
    def _display_check_list(self):
        dict = {}
        output = 'ATTR\tEMAILS\n'
        if os.path.exists(Ufeed.ufeed_dir + Ufeed.ufeed_check_file):
            f = open(Ufeed.ufeed_dir + Ufeed.ufeed_check_file, 'r')
            for eachline in f.readlines():
                if eachline != '':
                    output += '%s\t%s\n' % (eachline.split(' '*2)[0], eachline.split(' '*2)[1].split('\n')[0])
            f.close()

        self.write(generate_msg_page('/ufeed/all', output))
        
    def auto_check_feed(self, op=None, check_tasks=None, check_emails=None):
        arg_name = [['op:添加新字段或删除,查看已有字段','select','0:ADD','1:DEL','2:SEEK']
                    , ['check_tasks:例行回灌检查字段列表(每行一个)','textarea']
                    , 'check_emails:E-mails(逗号分隔,可以设置为None)']
        if op != '2' and check_tasks is None:
            self.print_items(0)
            self.write(generate_form('添加例行回灌检查字段', '/ufeed/auto_check_feed', arg_name))
        else:
            if op == '0':
                check_tasks = check_tasks.split('\r\n')
                check_dict = {}
                for each in check_tasks:
                    check_dict[each.strip()] = check_emails
                self._add_check_list(check_dict)
            elif op == '1':
                check_tasks = check_tasks.split('\r\n')
                self._del_check_list(check_tasks)
            else:
                self._display_check_list()

    def contact(self):
        self.print_items(0)
        self.write('<b>管理员</b><br><br>')
        self.write('<br>'.join(Ufeed.ufeed_emails.split(',')))
    
    def other_op(self):
        self.print_items(0)
        self.write('<br>请先登录')


HTTPRequestHandler.machine = MachineHandler
HTTPRequestHandler.slice = SliceHandler
HTTPRequestHandler.fifo = FifoHandler
HTTPRequestHandler.repository = RepositoryHandler
HTTPRequestHandler.op = OpHandler
HTTPRequestHandler.snapshot = SnapshotHandler
HTTPRequestHandler.writeproxy = WriteProxyHandler
HTTPRequestHandler.ufeed = UfeedHandler

tables_info = {} # table_id: table_name

def check_tables():
    global tables_info
    tables = Cli.list_table()
    for t in tables:
        tables_info[t.table_id] = t.name
    print tables_info

def init_fifos():
    global g_to_del_dests
    for repo in MachineManager.repo_writeproxys.keys():
        g_to_del_dests[repo] = [[], []]
    # TODO:get the number throught Cli
    total_dest_num = Settings.total_fifo_channel
    for machine in MachineManager.fifo_machines.keys():
        output = common.run_shell_deal_err('sh fifo_list.sh %s' % machine)
        fifos = [ [], [] ]
        output = output.split('\n')
        ftype = 0
        for line in output:
            if '' == line:
                continue
            items = line.strip().split(' ')
            if items[0] == 'Fifo':
                # Fifo newds-feed10 [Hold] 10010
                typename = items[1].split('-')[1][:4]
                if 'fifo' == typename:
                    ftype = 0
                elif 'feed' == typename:
                    ftype = 1
                else:
                    raise Exception('Error type name of fifo:%s' % items[1])
                fifos[ftype].append(FIFO.FIFO(DNS.resolve(machine, int(items[3])), items[1],
                        items[2] == '[Hold]', MachineManager.fifo_machines[machine]))
            else:# 'Dest'
                if items[1] != Settings.control_destname:
                    common._logger.info('bak_dest ignore: %s' % items[1])
                    continue
                # Dest tocur10 szjjh-ccdb1.szjjh01.baidu.com:9003 [Normal]
                addr = DNS.parse(items[2])
                fifos[ftype][-1].add_dest(items[1], addr, items[3] == '[Hold]')
                if MachineManager.writeproxys.has_key(addr.ip):
                    MachineManager.writeproxys[addr.ip].dests[ftype].append(fifos[ftype][-1].dests[-1])
                    MachineManager.writeproxys[addr.ip].valid_dest_count[ftype] += 1
                else:
                    repo = fifos[ftype][-1].machine.tag
                    g_to_del_dests[repo][ftype].append(fifos[ftype][-1].dests[-1])
        for ft in range(2):
            MachineManager.fifo_machines[machine].fifos[ft] = fifos[ft]
    sys.stdout.flush()

bak_repo_writeproxys = {}

def get_add_dest_ips(ftype, n):
    ''' get n exsiting wp_machine ips which have the least dests reffered;
        return list of ips
    '''
    ret_list = []
    h = [(wp.valid_dest_count[ftype], wp.ip) for wp in MachineManager.writeproxys.values()]
    heapq.heapify(h)
    for i in range(n):
        count, ip = heapq.heappop(h)
        ret_list.append(ip)
        MachineManager.writeproxys[ip].valid_dest_count[ftype] += 1
        heapq.heappush(h, (count+1, ip))
    return ret_list

def get_del_dest_ips(ftype, n):
    ''' get n exsiting wp_machine ips which have the most dests reffered;
        return list of ips
    '''
    ret_list = []
    h = [(wp.valid_dest_count[ftype], wp.ip) for wp in MachineManager.writeproxys.values()]
    heapq.heapify(h)
    for i in range(n):
        count, ip = heapq.nlargest(1, h)[0]
        ret_list.append(ip)
        h.remove((count, ip))
        MachineManager.writeproxys[ip].valid_dest_count[ftype] -= 1
        heapq.heappush(h, (count-1, ip))
    return ret_list

g_to_del_dests = {} # repo -> [ [fifo_dest, ...] , [feed_dest, ...] ]
former_failed_tasks = [] # [cmd_string]
g_wp_min_num_alarmed = {}
def run_fifoc_cmd(cmds, dest, ftype):
    common._logger.info("run : %s" % cmds)
    for i in range(len(cmds)):
#        if cmds[0].find('dest_replace') > 0:
#            raise Exception("Manuel Exception")
        output = common.run_shell_deal_err(cmds[0])
        common._logger.info("fifoc ok: %s" % cmds[0])
        if cmds[0].find('dest_replace') > 0:
            idx = dest.fifo.dests.index(dest)
            addr = DNS.parse(cmds[0].split(' ')[-1])
            dest.fifo.dests[idx].addr = addr
            MachineManager.writeproxys[addr.ip].dests[ftype].append(dest)
        cmds.pop(0)

def not_dealing_dest(d_dest):
    global former_failed_tasks
    for cmds, dest, ftype in former_failed_tasks:
        if d_dest is dest:
            return False
    return True

def check_fifo_dest():
    global former_failed_tasks 
    global bak_repo_writeproxys

    #do the former_failed_tasks
    failed_tasks = []
    common._logger.info('do %d former_failed_tasks' % len(former_failed_tasks))
    for cmds, dest, ftype in former_failed_tasks:
        try:
            output = run_fifoc_cmd(cmds, dest, ftype)
        except:
            failed_tasks.append((cmds, dest, ftype))
            common.print_exc_plus()
    former_failed_tasks = failed_tasks
    common._logger.info('finished the former_failed_tasks')

    MachineManager.machine_lock.acquire()
    try:
        for repo in MachineManager.repo_writeproxys.keys():
            #get broken writeproxy
            now = time.time()
            to_delete_dests = [ [], [] ]  # [ [fifo_dest, ...] , [feed_dest, ...] ]
            to_add_dest_WPips = [ [], [] ]    # [ [fifo_WriteProxy, ...] , [feed_WriteProxy] ]
            to_add_dest_num = [0, 0]

            if g_to_del_dests.has_key(repo):
                to_delete_dests[0].extend(g_to_del_dests[repo][0])
                to_delete_dests[1].extend(g_to_del_dests[repo][1])
            print "global 1fifo:", [ dest.addr.ip for dest in to_delete_dests[0]]
            print "global 1feed:", [ dest.addr.ip for dest in to_delete_dests[1]]
            sys.stdout.flush()

            # get left writeproxys
            wps = [ip for ip in MachineManager.repo_writeproxys[repo]]
            for ip in wps:
                if MachineManager.writeproxys[ip].last_check_time + Settings.writeproxy_timeout_sec < now:
                    MachineManager.repo_writeproxys[repo].remove(ip)
                    MachineManager.writeproxys.pop(ip)
                    common._logger.info('pop writeproxy ip: %s' % ip)

            # get dests_no_writeproxy
            for ip in MachineManager.repo_fifos[repo]:
                for ftype in range(2):
                    for fifo in MachineManager.fifo_machines[ip].fifos[ftype]:
                        for dest in fifo.dests:
                            if not MachineManager.writeproxys.has_key(dest.addr.ip) and not_dealing_dest(dest):
                                to_delete_dests[ftype].append(dest)
            print "dests_no_writeproxy 2fifo:", [ dest.addr.ip for dest in to_delete_dests[0]]
            print "dests_no_writeproxy 2feed:", [ dest.addr.ip for dest in to_delete_dests[1]]
            sys.stdout.flush()

            # switch dest from WP with more dests to WP with less dests
            WP_num = len(MachineManager.writeproxys)
            if 0 == WP_num:
                common._logger.fatal('There is no exsiting writeproxy machines!! Need to RESTART opccdb!!!')
                mon_alarm(Alarm(MON_FATAL, 'No exsiting writeproxy machines!', 'There is no exsiting writeproxy machines!!'))
                #TODO: deal with this situation
                return
            dests_per_WP = int(Settings.total_fifo_channel / WP_num)
            for ip, wp in MachineManager.writeproxys.items():
                for ftype in range(2):
                    diff = wp.valid_dest_count[ftype] - dests_per_WP
                    if diff >= 2: ###TODO:???
                        for i in range(diff):
                            dest = wp.dests[ftype].pop(-1)
                            MachineManager.writeproxys[ip].valid_dest_count[ftype] -= 1
                            to_delete_dests[ftype].append(dest)
                    elif diff <= -2:
                        to_add_dest_num[ftype] -= diff
                        
            print "average_diff 3fifo:", [ dest.addr.ip for dest in to_delete_dests[0]]
            print "average_diff 3feed:", [ dest.addr.ip for dest in to_delete_dests[1]]
            print "3fifo_add:", to_add_dest_num[0]
            print "3feed_add:", to_add_dest_num[1]
            sys.stdout.flush()
            if (len(to_delete_dests[0]) == 0 and len(to_delete_dests[1]) == 0
                    and to_add_dest_num[0] == 0 and to_add_dest_num[1] == 0):
                continue # to next repo

            if len(MachineManager.repo_writeproxys[repo]) < Settings.min_writeproxy_num:
                if not g_wp_min_num_alarmed.has_key(repo):
                    g_wp_min_num_alarmed[repo] = False
                if not g_wp_min_num_alarmed[repo]:
                    common._logger.info('Repo[%s]\'s writeproxys machines number <= %d,won\'t balance dests'%(repo,Settings.min_writeproxy_num))
                    print '%s writeproxys:'%repo, MachineManager.repo_writeproxys.keys()
                    mon_alarm(Alarm(MON_FATAL,'writeproxys number too small','Repo[ %s ]\'s writeproxys machines number <= %d,won\'t balance dests'%(repo,Settings.min_writeproxy_num)))
                    g_wp_min_num_alarmed[repo] = True
                continue
            else:
                g_wp_min_num_alarmed[repo] = False

            # do balancing
            for ftype in range(2):
                n = len(to_delete_dests[ftype]) - to_add_dest_num[ftype]
                if n < 0:
                    to_del_dest_ips = get_del_dest_ips(ftype, -n)
                    for ip in to_del_dest_ips:
                        to_delete_dests[ftype].append(MachineManager.writeproxys[ip].dests[ftype].pop(-1))
                to_add_dest_WPips[ftype] = get_add_dest_ips(ftype, len(to_delete_dests[ftype]))

                for i in range(len(to_add_dest_WPips[ftype])):
                    cmds = []
                    dest = to_delete_dests[ftype][i]
                    # fifoc fifoip:9000 fifo_hold fifo_name @ dest
                    #cmds.append('/home/work/fifo/bin/fifoc %s:9000 dest_hold %s %s' % (
                    #        dest.fifo.addr.ip, dest.fifo.name, dest.name))
                    # fifoc fifoip:9000 dest_replace fifo_name dest_name new_dest_ip new_dest_port
                    cmds.append('/home/work/fifo/bin/fifoc %s:9000 dest_replace %s %s %s:%s' % (
                            dest.fifo.addr.ip, dest.fifo.name, dest.name, 
                            to_add_dest_WPips[ftype][i], Settings.writeproxy_working_port))
                    # fifoc fifoip:9000 fifo_resume fifo_name
                    #cmds.append('/home/work/fifo/bin/fifoc %s:9000 dest_resume %s %s' % (
                    #        dest.fifo.addr.ip, dest.fifo.name, dest.name))
                    try:
                        run_fifoc_cmd(cmds, dest, ftype)
                    except:
                        common.print_exc_plus()
#                        common.print_exc()
                        former_failed_tasks.append((cmds, dest, ftype))
                        common._logger.info("fifoc failed: %s" % cmds[0])

            #TODO: succeed. Clean the garbage infomation.
            g_to_del_dests[repo] = [ [], [] ]
    except:
        common.print_exc_plus()
    finally:
        MachineManager.machine_lock.release()
    bak_repo_writeproxys = copy.deepcopy(MachineManager.repo_writeproxys)
    print 'bak_repo_writeproxys: ', bak_repo_writeproxys
    print 'valid_dest_count:'
    for repo in MachineManager.repo_writeproxys.keys():
        print [(ip,MachineManager.writeproxys[ip].valid_dest_count) for ip in MachineManager.repo_writeproxys[repo]]
    sys.stdout.flush()

class PeriodicChecker(threading.Thread):
    def __init__(self, period):
        self.states_infos = {}
        self.alarmed_sliceids = {}
        self.toalarm_slices = {}
        threading.Thread.__init__(self)
        self.period = period
        # initialise fifo infomation
        f = open('fifo_machine', 'r')
        tag = None
        for l in f:
            l = l.strip()
            if '' == l or '#' == l[0]:
                continue
            if 'repo' == l[:4]: 
                # repo: bailing
                tag = l.split(':')[1].strip()
                continue
            fm = FIFO.FIFOMachine(DNS.resolve(l, 9000), tag)
#            if not MachineManager.repo_fifos.has_key(tag):
#                MachineManager.repo_fifos[tag] = set()
            MachineManager.repo_fifos[tag].add(fm.addr.ip)
            MachineManager.fifo_machines[fm.addr.ip] = fm
        f.close()

    def run(self):
        global bak_repo_writeproxys
        # sleep a while for collecting WriteProxy machines
        if Settings.check_fifo_switch:
            common._logger.info('sleep for %d seconds. wait for writeproxy connecting' 
                    % self.period)
            time.sleep(self.period)
            common._logger.info('start initialize fifo information')
            init_fifos()
            common._logger.info('end initialize fifo information')
        bak_repo_writeproxys = copy.deepcopy(MachineManager.repo_writeproxys)
        while True:
            try:
                self._check_ing_state()
                #gen unitserver machine list
                machines=Cli.list_server()
                common._logger.info('unitserver number is %d'% len(machines))
                fd = open('ccdb_unitserver_host_list.conf.tmp','w')
                for m in machines:
                    fd.writelines(m.addr.ip + '\n')
                fd.close()
                file_hour_tag=time.strftime('%H',time.localtime(int(time.time())))[0]
                os.rename('ccdb_unitserver_host_list.conf.tmp','ccdb_unitserver_host_list.conf%s'%file_hour_tag)
                check_tables()
                time.sleep(self.period)
                if Settings.check_fifo_switch:
                    for repo in MachineManager.repo_writeproxys:
                        common._logger.info('repo:%s has %d writeproxys.' % 
                                (repo, len(MachineManager.repo_writeproxys[repo])))
                    check_fifo_dest()
                sys.stdout.flush()
            except:
                common.print_exc()

    def _check_ing_state(self):
        "check ing state is timeout"
        #repo

        for repo in tables_info.values():
            self.slices_and_state_count = Cli.list_slice(repo)
            if self.slices_and_state_count == None or len(self.slices_and_state_count[0]) == 0:
                common._logger.info(' Cli list slice on table [ %s ]failed, ignored it and check next repo...'%(repo))
                continue
            if not self.states_infos.has_key(repo):
                self.states_infos[repo] = {}
            if not self.alarmed_sliceids.has_key(repo):
                self.alarmed_sliceids[repo]= []
            if not self.toalarm_slices.has_key(repo):
                self.toalarm_slices[repo] = []
            for slice in self.slices_and_state_count[2]['normal']:
                if self.states_infos[repo].has_key(slice.slice_id):
                    del self.states_infos[repo][slice.slice_id]
                if slice.slice_id in self.alarmed_sliceids[repo]:
                    self.alarmed_sliceids[repo].remove(slice.slice_id)
            #state
            for state in self.slices_and_state_count[2].keys():
                if state == 'normal':
                    continue
                slices = self.slices_and_state_count[2][state]
                #slice
                for slice in slices:
                    #print "0.5 ", slice.state,slice.slice_id
                    if not self.states_infos[repo].has_key(slice.slice_id):
                        self.states_infos[repo][slice.slice_id] = {}
                    if state == 'creating snapshot' or state == 'deleting snapshot':
                        # show slice get ss infos
                        slice_infos = Cli.show_slice(repo, slice.slice_id)
                        if slice_infos is None or len(slice_infos) == 0:
                            common._logger.info(' Cli show slice [ %s ] on table [ %s ]failed, ignored it and check next slice...'%(slice.slice_id, repo))
                            continue
                        snapshots = slice_infos[2]

                        # get earliest !normal ss
                        earliest_snapshot = None
                        for ss in snapshots:
                            ssstate = state.split(' ')[0]
                            if ss.state[0:len(ssstate)] == ssstate:
                                if earliest_snapshot is None:
                                    earliest_snapshot = ss
                                elif time.mktime(time.strptime( ss.create_time )) < time.mktime(time.strptime( earliest_snapshot.create_time )):
                                    earliest_snapshot = ss 
                        # check haskey : state
                        if not self.states_infos[repo][slice.slice_id].has_key(state):
                            self.states_infos[repo][slice.slice_id] = {}
                            self.states_infos[repo][slice.slice_id][state] = {}
                        if earliest_snapshot is None :
                            self.states_infos[repo][slice.slice_id][state] = {}
                            continue

                        #print "1.5 earliest_snapshot:",earliest_snapshot.snapshot_id
                        # count ss !normal period
                        if not self.states_infos[repo][slice.slice_id][state].has_key(earliest_snapshot.snapshot_id):
                            self.states_infos[repo][slice.slice_id][state] = {}
                            self.states_infos[repo][slice.slice_id][state][earliest_snapshot.snapshot_id] = [1,earliest_snapshot.type]
                            if slice.slice_id in self.alarmed_sliceids[repo]:
                                self.alarmed_sliceids[repo].remove(slice.slice_id)
                        else:
                            self.states_infos[repo][slice.slice_id][state][earliest_snapshot.snapshot_id][0] += 1

                        snpt_period_num=self.states_infos[repo][slice.slice_id][state][earliest_snapshot.snapshot_id][0]
                        #print "1.6 snpt_period_num:",snpt_period_num

                        # check alarmed
                        if slice.slice_id in self.alarmed_sliceids[repo]:
                            continue

                        if earliest_snapshot.type == 'Slow' and earliest_snapshot.state == 'creating':
                            if snpt_period_num >= Settings.creating_slow_snapshot_alarm_period_num:
                                self.toalarm_slices[repo].append(slice)
                                common._logger.info('alarmed ing state slice[%s] snapshot[%s,%s]'% (slice.slice_id,earliest_snapshot.snapshot_id, earliest_snapshot.type))
                                self.alarmed_sliceids[repo].append(slice.slice_id)
                        else:
                            if snpt_period_num >= Settings.other_alarm_period_num:
                                self.toalarm_slices[repo].append(slice)
                                common._logger.info('alarmed ing state slice[%s] snapshot[%s,%s]'% (slice.slice_id, earliest_snapshot.snapshot_id, earliest_snapshot.type))
                                self.alarmed_sliceids[repo].append(slice.slice_id)
                    else:
                        #get node
                        slice_infos = Cli.show_slice(repo, slice.slice_id)
                        if slice_infos is None or len(slice_infos) == 0:
                            common._logger.info(' Cli show slice [ %s ] on table [ %s ]failed, ignored it and check next slice...'%(slice.slice_id, repo))
                            continue
                        units = slice_infos[1]
                        if len(units) == 0:
                            continue
                        node = '-'
                        for unit in units:
                            if unit.state[0:len(state)] == state:
                                node=unit.node
                        if node == '-':
                            continue
                        #check has state
                        #print "1.7 unit.node:",node , "slice state:", slice.state
                        if not self.states_infos[repo][slice.slice_id].has_key(state):
                            self.states_infos[repo][slice.slice_id] = {}
                            self.states_infos[repo][slice.slice_id][state] = 0
                            self.states_infos[repo][slice.slice_id]['node'] = node
                            if slice in self.alarmed_sliceids[repo]:
                                self.alarmed_sliceids[repo].remove(slice)
                        #check node
                        if not self.states_infos[repo][slice.slice_id].has_key('node'):
                            self.states_infos[repo][slice.slice_id]['node'] = '-'
                        if self.states_infos[repo][slice.slice_id]['node'] == node:
                            self.states_infos[repo][slice.slice_id][state] += 1
                        else:
                            self.states_infos[repo][slice.slice_id][state] = 0
                            self.states_infos[repo][slice.slice_id]['node'] = node
                            if slice in self.alarmed_sliceids[repo]: #remove alramed 
                                self.alarmed_sliceids[repo].remove(slice)
                            continue
                        #check timeout
                        stay_same_state_period = self.states_infos[repo][slice.slice_id][state]
                        #print "1.8 stay_same_state_period:",stay_same_state_period

                        # check alarmed
                        if slice.slice_id in self.alarmed_sliceids[repo]:
                            continue

                        if state == 'creating' :
                            if stay_same_state_period >= Settings.creating_alarm_period_num:
                                self.toalarm_slices[repo].append(slice)
                                common._logger.info('alarmed ing state slice[%s] state[%s] repo[%s]'% (slice.slice_id, slice.state, repo))
                                self.alarmed_sliceids[repo].append(slice.slice_id)
                        else:
                            if stay_same_state_period >= Settings.other_alarm_period_num:
                                self.toalarm_slices[repo].append(slice)
                                common._logger.info('alarmed ing state slice[%s] state[%s] repo[%s]'% (slice.slice_id, slice.state, repo))
                                self.alarmed_sliceids[repo].append(slice.slice_id)
            if len(self.toalarm_slices[repo]) != 0:
                alarm_msg = '\nslice状态存在异常，请及时检查\n库：%s\n'%(repo)
                alarm_msg=alarm_msg+'运维系统地址：http://%s:%s'%(platform.uname()[1],sys.argv[1].strip())+'\n\n'
                cellLen=17
                alarm_msg=alarm_msg+'异常slice列表如下：\n'
                alarm_msg=alarm_msg+'Slice ID'
                for i in range(len('Slice ID'),cellLen):
                    alarm_msg=alarm_msg+' '
                alarm_msg=alarm_msg+'   Slice State'
                for i in range(len('Slice State'),cellLen):
                    alarm_msg=alarm_msg+' '
                alarm_msg=alarm_msg+'\n'
                for i in range(0,cellLen):
                    alarm_msg=alarm_msg+'=='
                alarm_msg=alarm_msg+'\n'

                for slice in self.toalarm_slices[repo]:
                    #alarm_msg.join('<a href=http://%s:%s/slice/slice?repo=%s&slice_no=%s>%s</a>'
                    #        %(hostname,port,repo,slice.slice_id,slice.slice_id))
                    alarm_msg=alarm_msg+(str(slice.slice_id))
                    for i in range(len(str(slice.slice_id)),cellLen):
                        alarm_msg=alarm_msg+' '
                    alarm_msg=alarm_msg+'|'
                    alarm_msg=alarm_msg+slice.state
                    for i in range(len(slice.state),cellLen):
                        alarm_msg=alarm_msg+' '
                    alarm_msg=alarm_msg+'\n'
                    self.toalarm_slices[repo].remove(slice)
                mon_alarm(Alarm(MON_FATAL,'%s库Slice 状态异常！'%repo,alarm_msg))
                common._logger.info(alarm_msg)
                    
if __name__ == '__main__':
    if len(sys.argv) == 2:
        f = open('singleton', 'w')
        try:
            fcntl.lockf(f.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
        except:
            print 'A BailingOperation.py is already exits in this dir! exit...'
            sys.stdout.flush()
            os._exit(0)

        port = int(sys.argv[1])
        common.set_logger_level(Settings.log_level)
        check_tables()
        for repo in tables_info.values():
            MachineManager.repo_writeproxys[repo] = set()
            MachineManager.repo_fifos[repo] = set()
        PeriodicChecker(Settings.check_period_sec).start()
        if Settings.check_fifo_switch:
            MachineManager.WriteProxyListener(Settings.writeproxy_mon_port).start()
        if Settings.scheduler_switch:
            ImportTaskScheduler.ImportTaskScheduler().start()
        httpd = HttpServer(port, HTTPRequestHandler)
        socket.setdefaulttimeout(10)
        while True:
            try:
                httpd.handle_request()
            except:
                traceback.print_exc()
    else:
        print 'Usage: python BailingOperation.py <port>'
