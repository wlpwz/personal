import copy
import md5
import os
import popen2
import random
import re
import resource
import shutil
import signal
import socket
import sys
import threading
import time
import traceback
import urllib
import logging

class DBInfo:
    def __init__(self, addr_list, fifo_in, fifo_out):
        self.addr_list = addr_list
        self.tables = []
        self.fifo_in = fifo_in
        self.fifo_out = fifo_out

class DumpFile:
    def __init__(self, filename):
        self.filename = filename
        self.tmpname = '/tmp/' + ''.join([hex(random.randrange(256))[2:] for i in range(8)])
        self.committed = False
        self.f = open(self.tmpname, 'w')

    def __getattr__(self, name):
        if name == 'close':
            return self.close
        elif name == 'commit':
            return self.commit
        else:
            return getattr(self.f, name)

    def __del__(self):
        self.close()

    def commit(self):
        self.committed = True

    def close(self):
        if self.f is not None:
            self.f.close()
            self.f = None
            if self.committed:
                try:
                    os.system('mv "%s" "%s"' % (self.tmpname, self.filename))
                    return
                except:
                    traceback.print_exc()
            try:
                os.remove(self.tmpname)
            except:
                traceback.print_exc()
            
def print_exc():
    try:
        traceback.print_exc()
    except:
        print 'Error in print_exc'
    sys.stdout.flush()

def print_exc_plus():
    tb = sys.exc_info()[2]
    while tb.tb_next:
        tb = tb.tb_next
    stack = []
    f = tb.tb_frame
    while f:
        stack.append(f)
        f = f.f_back
    stack.reverse()
    traceback.print_exc()
    print "Locals by frame, innermost last"
    for frame in stack:
        print
        print "Frame %s in %s at line %s" % (frame.f_code.co_name,
                                             frame.f_code.co_filename,
                                             frame.f_lineno)
        for key,value in frame.f_locals.items():
            print "\t%20s = " % key,
            try:
                print value
            except:
                print "<ERROR WHILE PRINTING VALUE>"
    sys.stdout.flush()

def format_time(t, format = '%Y-%m-%d %H:%M:%S'):
    return time.strftime(format, time.localtime(t))

def parse_time(time_str, format = '%Y-%m-%d %H:%M:%S'):
    return int(time.mktime(time.strptime(time_str.strip(), format)))

def log(str):
    print '[%s]:%s' % (format_time(int(time.time())), str)
    
class ShellException(Exception):
    def __init__(self, status, error):
        Exception.__init__(self, error)
        self.status = status

def run_shell(cmd, input=None, time_limit=60):
    cmd = 'sh -c \'%s\'' % cmd.replace('\'', '\'"\'"\'')
    f = popen2.Popen3(cmd, True)
    try:
        if input is not None:
            f.tochild.write(input)
        f.tochild.close()
        output = f.fromchild.read()
        error = f.childerr.read()
    finally:
        f.tochild.close()
        f.fromchild.close()
        f.childerr.close()
        status = f.wait()
    if status:
        raise ShellException(status, error)
    return output

def run_shell_deal_err(cmd, input=None):
    cmd = 'sh -c \'%s\'' % cmd.replace('\'', '\'"\'"\'')
    f = popen2.Popen3(cmd, True)
    try:
        if input is not None:
            f.tochild.write(input)
        f.tochild.close()
        output = f.fromchild.read()
        error = f.childerr.read()
    finally:
        f.tochild.close()
        f.fromchild.close()
        f.childerr.close()
        status = f.wait()
    if status:
        raise ShellException(status, error)
    if 'Operation complete successfully.\n' != error and '' != error:
        raise ShellException(status, error)
    return output

def run_and_wait(path, args):
    pid = os.fork()
    if pid > 0:
        while True:
            try:
                pid, status = os.waitpid(pid, 0)
                return status
            except:
                print_exc()
    try:
        for i in range(3, 100):
            try:
                os.close(i)
            except OSError:
                pass
        os.execvp(path, args)
        os._exit(os.EX_SOFTWARE)
    except:
        print_exc()
        os._exit(os.EX_SOFTWARE)

def all_md5():
    f = os.popen('openssl md5 *.sh *.py | awk \'{print $2}\' | sort | paste -s -d " "')
    try:
        return f.read()
    finally:
        f.close()

def replace(s, dict):
    ret = []
    for term in re.split(r'(\$[A-Z_]+)', s):
        if len(term) > 0 and term[0] == '$' and term[1:] in dict:
            term = str(dict[term[1:].upper()])
        ret.append(term)
    return ''.join(ret)

def expand(s):
    if s == '':
        return
    for t in s.split(','):
        t = t.split('-', 1)
        if len(t) == 1:
            yield t[0]
        else:
            for it in range(int(t[0]), int(t[1]) + 1):
                yield str(it)

def expand_hostname(s):
    match = re.search(r'\[[0-9\,\-]+\]', s)
    if match is None:
        yield s
    else:
        for t in expand(match.group(0)[1:-1]):
            yield s[:match.start()] + t + s[match.end():]

def to_bytes(s):
    if s.endswith('G') or s.endswith('g'):
        return int(float(s[:-1]) * 1024 * 1024 * 1024)
    elif s.endswith('M') or s.endswith('m'):
        return int(float(s[:-1]) * 1024 * 1024)
    elif s.endswith('K') or s.endswith('k'):
        return int(float(s[:-1]) * 1024)
    else:
        return int(s)

def concat_seq(seq):
    ret = []
    a = -100
    b = -100
    for i in seq:
        if b + 1 == i:
            b = i
        else:
            if b >= 0:
                if a == b:
                    ret.append('%d' % a)
                else:
                    ret.append('%d-%d' % (a, b))
            a = i
            b = i
    if b >= 0:
        if a == b:
            ret.append('%d' % a)
        else:
            ret.append('%d-%d' % (a, b))
    return ','.join(ret)

def sendmail(to_addr_list, title, content):
    stdin = os.popen('mail -s "%s" "%s"' % (title, ','.join(to_addr_list)), 'w')
    try:
        stdin.write(content)
    finally:
        stdin.close()

def notify(receiver_list, title, content):
    os.system('sh notify.sh "%s" "%s" "%s"' % (','.join(receiver_list), title, content))

def read_file(filename):
    f = open(filename, 'r')
    try:
        return f.read()
    finally:
        f.close()

class LinkedList:
    class Node:
        def __init__(self, value):
            self.value = value
            self.next = None

    def __init__(self):
        self._head = None
        self._tail = None
        self.length = 0

    def push(self, value):
        node = LinkedList.Node(value)
        if self._head is None:
            self._head = node
        if self._tail is not None:
            self._tail.next = node
        self._tail = node
        self.length += 1

    def pop(self):
        ret = self.head()
        self._head = self._head.next
        if self._head is None:
            self._tail = None
        self.length -= 1
        return ret

    def empty(self):
        return self._head is None

    def head(self):
        if self._head is None:
            return None
        return self._head.value

    def __len__(self):
        return self.length

    def __iter__(self):
        t = self._head
        while t:
            yield t.value
            t = t.next

class Queue:
    def __init__(self):
        self.length = 0
        self.offset = 0
        self.chunks = []
            
    def __len__(self):
        return self.length

    def __iter__(self):
        for i in range(len(self.chunks)):
            chunk = self.chunks[i]
            for j in xrange(i == 0 and self.offset or 0, len(chunk)):
                yield chunk[j]

    def push(self, value):
        self.length += 1
        if not self.chunks or len(self.chunks[-1]) >= 1000:
            self.chunks.append([])
        self.chunks[-1].append(value)

    def pop(self):
        if not self.chunks or self.offset >= len(self.chunks[0]):
            return None
        ret = self.chunks[0][self.offset]
        self.offset += 1
        if self.offset >= 1000:
            del self.chunks[0]
            self.offset = 0
        return ret

    def head(self):
        if not self.chunks or self.offset >= len(self.chunks[0]):
            return None
        return self.chunks[0][self.offset]

class Object:
    pass

def get_logger():
    logger = logging.getLogger('operation')
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("[%(asctime)s][%(levelname)s][%(filename)s][%(lineno)d][%(funcName)s]:%(message)s" , '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(ch)
    return logger
    
def set_logger_level(log_level):
    if log_level == 'fatal':
        _logger.setLevel(logging.FATAL)
    elif log_level == 'error':
        _logger.setLevel(logging.ERROR)
    elif log_level == 'warn':
        _logger.setLevel(logging.WARN)
    elif log_level == 'info':
        _logger.setLevel(logging.INFO)
    elif log_level == 'debug':
        _logger.setLevel(logging.DEBUG)

_logger = get_logger()

def remove_baidu_tail(host):
    return host.endswith('.baidu.com') and host[:-len('.baidu.com')] or host

