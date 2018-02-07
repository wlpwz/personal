# coding=gbk
import sys
import os

version = '1.0.0.8'
############################################################## 
# scheduler
#是否后台开启倒库任务
scheduler_switch = False
#倒库目标slice号范围起始slice号
slice_start = 3602
#倒库目标slice号范围结束slice号
slice_end = 19810
#运维系统与倒库任务交互的端口
schedule_port = 12345
#倒库库种
import_repo = 'bailing'
##############################################################
#check option
#是否后台开启fifo dest平衡
check_fifo_switch = False
#检测周期 单位s
check_period_sec = 2
#运维接收wp心跳端口
writeproxy_mon_port = 55716
#wp工作端口，接收数据的写端口
writeproxy_working_port = 9005
#wp心跳超时时间，单位s
writeproxy_timeout_sec = 30 * 60
#fifo channel总数，都表混布中不一定与dest数目一致（可能会一个channel多个库种dest）
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
#是否为含有Quota信息的集群，取决于Cli和Master版本
has_quota = False
#wp机器数目的下限，低于此，运维后台不做dest平衡化处理，并发出邮件报警
min_writeproxy_num = 88
############################################################## 
#check slice state
#creating状态的报警周期，每个周期：check_period_sec
creating_alarm_period_num = 5
#slice处于creating 慢snapshot的报警周期数
creating_slow_snapshot_alarm_period_num = 2
#slice其他状态的报警时间
other_alarm_period_num = 2
#运维报警邮件接收地址，多人列表的话用,分隔
emails ='wangfeng07@baidu.com'
