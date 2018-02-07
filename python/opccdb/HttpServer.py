import BaseHTTPServer
import cgi
import new
import sys
import traceback
import urlparse
import Cookie
import time

class HttpServer:
    class ServerHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
        def do_GET(self):
            try:
                p = urlparse.urlparse(self.path)
                self.handle_request(p[2], p[4])
            except:
                self.send_error(500)
                traceback.print_exc()

        def do_POST(self):
            try:
                len = self.headers.getheader('content-length')
                self.handle_request(self.path, self.rfile.read(int(len)))
            except:
                self.send_error(500)
                traceback.print_exc()

        def handle_request(self, path, query):
            handler = self.get_handler()
            method = handler
            for name in path.split('/'):
                if len(name) > 0:
                    handler = method
                    if not hasattr(handler, name):
                        self.send_response(404)
                        return
                    method = getattr(handler, name)
            if method == handler:
                if not hasattr(handler, 'default'):
                    self.send_response(404)
                    return
                method = getattr(handler, 'default')
            handler = handler()
            setattr(handler, 'write', lambda str : self.write(str))
            setattr(handler, 'set_header', lambda name, value : self.set_header(name, value))
            setattr(handler, 'redirect', lambda path : self.redirect(path))
            setattr(handler, 'disable_cache', lambda : self.disable_cache())
            setattr(handler, 'read_cookie', lambda name : self.read_cookie(name))
            self.header_sent = False
            if "Cookie" in self.headers:
                c = Cookie.SimpleCookie(self.headers["Cookie"])
                if 'passcode' in c:
                    c['passcode']['expires'] = time.strftime("%a, %d-%b-%Y %H:%M:%S GMT", time.gmtime(time.time() + 20 * 60))
                self.headers = {}
                self.set_header('Set-Cookie', c.output(header=''))
                self.headers['Cookie'] = c.output(header='')
            else:
                self.headers = {}
                
            params = {}
            for k, v in cgi.parse_qs(query).items():
                print k, v
                if len(v) == 1:
                    params[k] = v[0]
                else:
                    params[k] = v
            method(*[handler], **params)

        def redirect(self, path):
            self.send_response(301)
            self.send_header('Location', path)
            self.end_headers();

        def disable_cache(self):
            self.set_header('cache-control', 'no-cache')

        def write(self, str):
            if not self.header_sent:
                self.send_response(200)
                self.header_sent = True
                if not 'content-type' in self.headers:
                    self.headers['content-type'] = 'text/html'
                if not 'cache-control' in self.headers:
                    self.headers['cache-control'] = 'no-cache'
                for k, v in self.headers.items():
                    self.send_header(k, v)
                self.end_headers()
            self.wfile.write(str)

        def set_header(self, name, value):
            self.headers[name.lower()] = value

        def read_cookie(self, name):
            if "Cookie" in self.headers:
                c = Cookie.SimpleCookie(self.headers["Cookie"])
                if name in c:
                    return c[name].value
                else:
                    return None
            return None
        
        def address_string(self):
            host, port = self.client_address[:2]
            return "%s:%s" % (host, port)
        
    def __init__(self, port, handler):
        self.server = BaseHTTPServer.HTTPServer(('', port), new.classobj('HttpServerHandler%d' % id(self), (HttpServer.ServerHTTPRequestHandler,), {'get_handler' : lambda self : handler}))

    def handle_request(self):
        self.server.handle_request()
