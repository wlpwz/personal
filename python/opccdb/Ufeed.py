import popen2

ufeed_dir = '/home/work/unified_feed/'
ufeed_emails = 'dingtao01@baidu.com'
ufeed_check_file = 'check_list'

class Task:
    def __init__(self, name, uptime, checktime, cachedivtime, feedtime):
        self.name = name
        self.uptime = uptime
        self.checktime = checktime
        self.cachedivtime = cachedivtime
        self.feedtime = feedtime

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
#    if status:
#        raise ShellException(status, error)
    return output

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
                temp = arg_list[i][0].split(':')
                t = ''
                for i in range(1, len(temp)):
                    if t != '':
                        t += ':'
                    t += temp[i]
                html += '<label>%s</label><br><textarea rows="10" cols="30" name="%s"></textarea><br>' % (t, temp[0])
    html += '<input type="submit" value="Submit"/><br></form></div>'
    return html

