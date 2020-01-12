from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer
import json, logging, logging.config
from qc import queryCorrect

qc = queryCorrect()

log_conf_file = 'log4ic.conf'
logging.config.fileConfig(log_conf_file)

class Handler(RequestHandler):
    def post(self):
        try:
            req_dict = json.loads(self.request.body.decode('utf-8'))
            text = json.loads(self.request.body.decode('utf-8'))["request"]["p"]["query"]
            self.set_header('Content-Type', 'application/json')
            #q = qc.correct(text)  #;print(q); exit()
            r = qc.run(req_dict)  #;print(json.dumps(r, ensure_ascii=False)); exit()
            res = json.dumps({"header": {}, "response": {"err_no": "0", "err_msg": "", "results": r}}, ensure_ascii=False)
            self.write(res)
        except Exception as e:
            logging.warn('__post_failed, req=%s, exception=[%s]' % (json.dumps(req_dict,ensure_ascii=False), str(e)))

if __name__ == '__main__':
    numworkers = 1
    app = Application([(r'/query_correct', Handler)], debug=False)
    http_server = HTTPServer(app)
    http_server.bind(51668)
    http_server.start(numworkers)
    logging.info('__query_correct_server_running__ num_workers: %s' % numworkers)
    IOLoop.current().start()
