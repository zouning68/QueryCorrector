import Levenshtein, json, logging, sys, traceback, time, copy, jieba
#from es_utils import ElasticObj
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
        #self.right = customize_set("./right")
        #self.wrong = customize_set("./wrong")
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
        try:
            corrected_sent, detail = self.corrector.correct(text)
            return corrected_sent, detail
        except Exception as e:
            logging.warning('correct_err=%s' % repr(e)); print(traceback.format_exc())
            return text, []

    def run(self, req_dict):
        result = {}
        self.on_correct_begin()
        self.logs['req_dict'] = req_dict
        query = req_dict['request']['p']['query']
        result["original_query"], result["corrected_query"], result["detail"] = query, "", []
        query = clean_query(query)
        try:
            if is_name(query): #or is_company(query):      # 人名或公司名不纠错
                result["corrected_query"], result["detail"] = query, []
            else:
                result["corrected_query"], result["detail"] = self.correct(query)  # 开始纠错
            #print(json.dumps(result, ensure_ascii=False))
        except Exception as e:
            logging.warning('run_err=%s' % repr(e)); print(traceback.format_exc())
        self.logs['result'] = result
        self.logs['senten2term'] = self.corrector.senten2term
        self.logs['query_entitys'] = self.corrector.query_entitys
        self.logs['maybe_errors'] = self.corrector.maybe_errors
        self.on_correct_end()
        #print(self.logs); exit()
        return result

if __name__ == '__main__':
    try: que = sys.argv[1]
    except: que = "pptv,andorid"
    req_dict = {"header": {},"request": {"c": "", "m": "query_correct", "p": {"query": que}}}
    qc = queryCorrect()
    print(qc.run(req_dict))
    #print(qc.ngrams.get("市场", 0))


