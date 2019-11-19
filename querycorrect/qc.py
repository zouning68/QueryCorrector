import Levenshtein, json, logging, sys, traceback, time, copy, jieba
from es_utils import ElasticObj
from correct import Corrector
from utils import is_name, clean_query
from company import is_company

def resolve_search(search):
    res, s = [], 0
    if search and search['hits']['hits']:
        for hit in search['hits']['hits']:
            res.append((hit['_source']['candidate_query'], hit['_source']['candidate_query_freq'])) ; #print(json.dumps(hit, ensure_ascii=False))
            s += hit['_source']['candidate_query_freq']
    return res, s

def customize_set(file_path):
    res = []
    try:
        res = [e.strip() for e in open(file_path, encoding='utf8').readlines() if e.strip() != '']
    except Exception as e:
        logging.warn('customize_set_err=%s' % repr(e))
    return res

def editdist(word1, word2):
    w1, w2 = str(word1).replace(" ", ""), str(word2).replace(" ", "")
    d = round(Levenshtein.ratio(w1, w2), 3)
    return d

#print(customize_set('./right'));exit()

class queryCorrect:
    def __init__(self):
        self.VERSION = 'query_correct_1'
        self.logs = {}
        #self.es_obj = ElasticObj("candidate_query")
        self.right = customize_set("./right")
        self.wrong = customize_set("./wrong")
        self.corrector = Corrector()
        logging.info('init queryCorrect ok, version=[%s]' % self.VERSION)

    def on_correct_begin(self):
         logging.debug('on_correct_begin')
         self.t_begin = time.time()

    def on_correct_end(self):
         logging.debug('on_correct_end')
         phead = '[on_correct_end] | log_info=%s | cost=%.3fs'
         logging.info(phead % (json.dumps(self.logs, ensure_ascii=False), (time.time()-self.t_begin)))

    def correct(self, text):
        res, dist, debug, origion_text = "", [], [], copy.deepcopy(text)
        text = clean_query(text)
        try:
            corrected_sent, detail = self.corrector.correct(text)
            res = [(corrected_sent, 0.5), detail]
        except Exception as e:
            logging.warn('correct_err=%s' % repr(e)); print(traceback.format_exc())
        return res

    def run(self, req_dict):
        self.on_correct_begin()
        self.logs['req_dict'] = req_dict
        callback_response = {}
        try:
            q = req_dict['request']['p']['query']
            if is_name(q) or is_company(q):      # 人名或公司名不纠错
                res = [(q, 1), []]
            else:
                res = self.correct(q)  # 开始纠错
            if res: callback_response['correct_result'] = res
            else: callback_response['correct_result'] = [(q, 0), []]
            #print(json.dumps(callback_response, ensure_ascii=False))
        except Exception as e:
            logging.warn('run_err=%s' % repr(e)); print(traceback.format_exc())
            callback_response['correct_result'] = [(q, -1), []]
        self.logs['result'] = callback_response
        self.on_correct_end()
        #print(self.logs); exit()
        return callback_response

if __name__ == '__main__':
    try: que = sys.argv[1]
    except: que = "淮阴工学院,点匠科技"
    req_dict = {"header": {},"request": {"c": "", "m": "query_correct", "p": {"query": que}}}
    qc = queryCorrect()
    print(qc.run(req_dict))
    #print(qc.ngrams.get("市场", 0))


