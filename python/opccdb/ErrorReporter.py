# -*- coding: GB18030 -*-
import threading
import traceback
import platform

import Settings
from common import *
from Thread import *

#f = open('hdfs/bailing/maintainers')
#try:
#    maintainers = [line.strip() for line in f]
#    maintainers = [maintainer for maintainer in maintainers if len(maintainer) > 0 and maintainer[0] != '#']
#    if len(maintainers) == 0:
#        raise Exception('No maintainers')
#finally:
#    f.close()

_lock = threading.Lock()
_alarms = []

MON_NOTICE = 'Notice'
MON_ERROR = 'Error'
MON_FATAL = 'Fatal'
_host_name = platform.uname()[1]

class Alarm:
    def __init__(self, type, title, body):
        self.type = type
        self.title = title
        self.body = body
    
    def send(self):
        self._send_mail()
#        if self.type == MON_FATAL:
#            self._send_sms()
            
    def _send_mail(self, emails=None):
        if emails is None:
            emails = Settings.emails
        emails = emails.split(',')
        sendmail(emails, '[%s][opccdb][%s][%s][%s]' 
                  % (self.type, _host_name, self.title, format_time(time.time(), '%Y-%m-%d_%H:%M:%S')), self.body)

    def _send_sms(self):
        msg = self.title + ':'+ self.body
        msg = msg.replace('\n',' ')
        if len(msg) > 100:
            msg = msg[:97] + '...'
        os.system('/bin/gsmsend -s %s "%s@%s"' % (Settings.gsm_server, Settings.gsm_notifier, msg))

class ErrorReporterThread(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        while not self.interrupt_flag:
            self.sleep(60)
            _lock.acquire()
            global _alarms
            to_alarm = _alarms
            _alarms = []
            _lock.release()
            while True:
                try:
                    for alarm in to_alarm:
                        alarm.send()
                    break
                except:
                    traceback.print_exc()
                    self.sleep(10)
error_reporter = ErrorReporterThread()
error_reporter.start()

def mon_alarm(alarm):
    _lock.acquire()
    try:
        _alarms.append(alarm)
    finally:
        _lock.release()

