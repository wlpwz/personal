# coding=gbk
import sys
import os

version = '1.0.0.8'
############################################################## 
# scheduler
#�Ƿ��̨������������
scheduler_switch = False
#����Ŀ��slice�ŷ�Χ��ʼslice��
slice_start = 3602
#����Ŀ��slice�ŷ�Χ����slice��
slice_end = 19810
#��άϵͳ�뵹�����񽻻��Ķ˿�
schedule_port = 12345
#�������
import_repo = 'bailing'
##############################################################
#check option
#�Ƿ��̨����fifo destƽ��
check_fifo_switch = False
#������� ��λs
check_period_sec = 2
#��ά����wp�����˿�
writeproxy_mon_port = 55716
#wp�����˿ڣ��������ݵ�д�˿�
writeproxy_working_port = 9005
#wp������ʱʱ�䣬��λs
writeproxy_timeout_sec = 30 * 60
#fifo channel����������첼�в�һ����dest��Ŀһ�£����ܻ�һ��channel�������dest��
total_fifo_channel = 3000
control_destname = 'tocur'
# op_big
run_root = os.path.split(os.path.realpath(sys.argv[0]))[0]
op_passcode = 'n7t9'
dump_interval_sec = 3 * 60
############################################################## 
gsm_server = 'tc-sys-monitor00.tc:15001'
gsm_notifier = '13000000000,13000000000'
mon_mail_dir = '%s/hdfs/mon_mail' % run_root
############################################################## 
#log settings
log_level = 'debug'

dump_machine_dir = '%s/machines/' % run_root
#�Ƿ�Ϊ����Quota��Ϣ�ļ�Ⱥ��ȡ����Cli��Master�汾
has_quota = False
#wp������Ŀ�����ޣ����ڴˣ���ά��̨����destƽ�⻯�����������ʼ�����
min_writeproxy_num = 88
############################################################## 
#check slice state
#creating״̬�ı������ڣ�ÿ�����ڣ�check_period_sec
creating_alarm_period_num = 5
#slice����creating ��snapshot�ı���������
creating_slow_snapshot_alarm_period_num = 2
#slice����״̬�ı���ʱ��
other_alarm_period_num = 2
#��ά�����ʼ����յ�ַ�������б�Ļ���,�ָ�
emails ='wangfeng07@baidu.com'
