from elasticsearch import Elasticsearch
import json, logging, traceback, requests, sys, math, os, time, re
from data_utils import read_file
from elasticsearch.helpers import bulk
from utils import clean_query
from config import config

index_mappings = {
    "mappings": {
        "_source": {
            "enabled": True
        },
        "properties": {
            "candidate_query": {"type": "text"},
            "candidate_query_chars": {"type": "text"},
            "candidate_query_freq": {"type": "integer"},
            "candidate_query_length": {"type": "integer"}
        }
    }
}

def init_log():
    cur_file = os.path.split(__file__)[-1].split(".")[0]
    logging.basicConfig(level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filename='./'+cur_file+'.log',
            filemode='a')

class query:
     def __init__(self, queryType = 'bool'):
         self.query = {}
         self.queryType = queryType
         self.query['query'] = {}
         self.query['query'][self.queryType] = {}
         self.query['query'][self.queryType]['must'] = []
         self.query['query'][self.queryType]['must_not'] = []
         self.query['query'][self.queryType]['should'] = []
         self.hasQuery = False

     def addQuery(self, category = '', k = '', v = '', gte = '', lte = '', type = '', query_map = {}):
         if category == 'term':
             Map = {'term': {k: v}}
         elif category == 'range':
             Map = {'range': {k: {'gte': gte, 'lte': lte}}}
         elif category == 'map':
             Map = query_map
         elif category == 'terms':
             Map = {'terms': {k: v}}
         else:
             return
         if type == 'must':
             self.query['query'][self.queryType]['must'].append(Map)
             self.hasQuery = True
         elif type == 'must_not':
             self.query['query'][self.queryType]['must_not'].append(Map)
             self.hasQuery = True
         elif type == 'should':
             self.query['query'][self.queryType]['should'].append(Map)
             self.hasQuery = True

class ElasticObj:
    def __init__(self, index_name, index_type = "_doc", env = "local"):
        try:
            self.index_name = index_name
            self.index_type = index_type
            ES_DRESS = ["192.168.12.18:9100"] if env == "online" else ['192.168.7.206:9100']
            self.url = 'http://' + ES_DRESS[0] + '/' + str(index_name) + '/_search'
            self.esObj = Elasticsearch(
                ES_DRESS,
                # sniff before doing anything
                sniff_on_start=True,
                # refresh nodes after a node fails to respond
                sniff_on_connection_fail=True,
                # and also every 60 seconds
                sniffer_timeout=6
            )
        except Exception as e:
            logging.warning('ES_init_err=%s' % repr(e)); print(traceback.format_exc())

    def create_index(self):
        try:
            if self.esObj.indices.exists(index=self.index_name) is not True:
                res = self.esObj.indices.create(index=self.index_name, body=index_mappings)
                print('index is create->%s, %s' % (res, json.dumps(index_mappings, ensure_ascii=False)))
                logging.info('index is create->%s, %s' % (res, json.dumps(index_mappings, ensure_ascii=False)))
            else:
                print('index is already exist, %s' % (json.dumps(index_mappings, ensure_ascii=False)))
                logging.info('index is already exist, %s' % (json.dumps(index_mappings, ensure_ascii=False)))
        except Exception as e:
            logging.warning('create_index_err=%s' % repr(e)); print(traceback.format_exc())

    def delete_index(self):
        try:
            if self.esObj.indices.exists(index=self.index_name):
                res = self.esObj.indices.delete(index=self.index_name)
                print('index is delete->', res)
                logging.info("index is delete->%s", res)
            else:
                print('index is not exist')
                logging.info("index is not exist")
        except Exception as e:
            logging.warning('create_index_err=%s' % repr(e)); print(traceback.format_exc())

    def index_exist(self):
        return self.esObj.indices.exists(index=self.index_name)

    def update_index(self):
        try:
            self.delete_index()     # 删除索引
            self.create_index()     # 创建索引
            # ******************** 更新索引数据 ************************
            candidate_query = read_file(config.candidate_query_path)
            _id_ = 0
            for k, v in candidate_query.items():
                #print("%s\t%d\t%s" % (k, int(v), ' '.join(list(k)))); exit()
                obj_map = {
                    "candidate_query": k,
                    "candidate_query_chars": " ".join(list(k)),
                    "candidate_query_freq": int(v)
                }
                r = self.esObj.index(index=self.index_name, doc_type=self.index_type, body=obj_map, id=_id_)
                _id_ += 1
                print(json.dumps(r, ensure_ascii=False))
        except Exception as e:
            logging.warning('update_index_err=%s' % repr(e)); print(traceback.format_exc())

    def update_index_batch(self, batch_size = 100):
        try:
            _id_, t0 = 0, time.time()
            self.delete_index()  # 删除索引
            self.create_index()  # 创建索引
            # ******************** 更新索引数据 ************************
            candidate_query = read_file(config.candidate_query_path)
            candidate_query_keys = list(candidate_query.keys())
            total = len(candidate_query_keys)
            batchs = math.ceil(len(candidate_query_keys) / batch_size)
            for i in range(batchs):
                actions = []
                batch = candidate_query_keys[i*batch_size:(i+1)*batch_size]
                for e in batch:
                    obj_map = {
                            "_index": self.index_name,
                            "_type": self.index_type,
                            "_id": _id_,
                            "_source": {
                                "candidate_query": e,
                                "candidate_query_chars": " ".join(list(e)),
                                "candidate_query_freq": int(candidate_query[e]),
                                "candidate_query_length": len(e)
                                }
                            }
                    _id_ += 1
                    actions.append(obj_map)
                #print(json.dumps(actions, ensure_ascii=False)); exit()
                res = bulk(self.esObj, actions, index=self.index_name, raise_on_error=True)
                print("total: %d\tcurrent batch:%d\ttotal batch: %d\tsuccess: %s\tfailed: %s" % (total, i+1, batchs, res[0], res[1]))
                logging.info("total: %d\tcurrent batch:%d\ttotal batch: %d\tsuccess: %s\tfailed: %s" % (total, i+1, batchs, res[0], res[1]))
            print("time cost: %fs" % (time.time() - t0)); logging.info("time cost: %fs" % (time.time() - t0))
        except Exception as e:
            logging.warning('update_index_batch_err=%s' % repr(e)); print(traceback.format_exc())

    def get_all_data(self, size=10000):
         try:
             searched = self.esObj.search(index=self.index_name, body=json.dumps({'query': {'match_all': {}}}, ensure_ascii=False), size=size)
             return searched
         except Exception as e:
             logging.warn('get_all_data_err=%s' % repr(e)); print(traceback.format_exc())
             return None

    def getSortedDataByURL(self, query_map={'match_all': {}}, functions_list=[], Size=10, _from=0, bm='sum', sort_=None, sm='sum'):
         try:
             body = {
                     "query": {
                         "function_score": {
                             "query": query_map,
                             "functions": functions_list,
                             "score_mode": sm,
                             "boost_mode": bm,
                             "boost": 1,
                             }
                         },
                     "from": _from*Size,
                     "size": Size
                     }
             if sort_: body['sort'] = sort_
             query = json.dumps(body)
             headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
             response = requests.post(self.url, data=query, headers=headers)
             #print(json.dumps(json.loads(query), ensure_ascii=False, indent=4), '\n\n', response.text); exit()
             return json.loads(response.text)
         except Exception as e:
             logging.warn('getDataByURL_err=%s' % repr(e)); print(traceback.format_exc())
             return None

    def search(self, text, _size=10):
         try:
             text = clean_query(text)
             len_text, len_min, len_max = len(text), max(0, len(text) - 2), len(text) + 2
             #print(text, len_text, len_min, len_max); exit()
             text = re.sub(r"([\+\^\(\)\*])", r"\\\1", text)    # 搜索query特殊字符的转义处理
             chars = " ".join(list(text))
             q = query()
             qm = {"query_string": {"query": "candidate_query_chars:(" + chars + ") candidate_query:(" + text + ")^10000", "minimum_should_match": "50%"}}
             q.addQuery(category='map', query_map=qm, type='should')
             q.addQuery(category='range', k='candidate_query_length', gte=len_min, lte=len_max, type='must')
             functions = []; functions.append({'script_score': {"script": "(Math.log1p(doc['candidate_query_freq'].value)*1) / 1"}, 'weight': 0.6})
             searched = self.getSortedDataByURL(q.query['query'], Size=_size, functions_list=functions)
             return searched
         except Exception as e:
             logging.warn('search_err=%s' % repr(e)); print(traceback.format_exc())
             return None

if __name__ == "__main__":
    init_log()
    try: que = sys.argv[1]
    except: que = "量化 c++"
    es_obj = ElasticObj("candidate_query")

    #es_obj.update_index()      # ****** 更新索引操作 *******
    if que == "gxs":
        es_obj.update_index_batch(50000)    # ****** 批量更新索引操作 *******

    #es_obj.delete_index()
    #es_obj.create_index()
    #sech = es_obj.get_all_data(5); print("%s\n%s" % (sech['hits']['total'], json.dumps(sech, ensure_ascii=False)))
    #q = query(); q.addQuery(category="terms", k="candidate_query", v=["研", "法", "测", "试", "究", "员"], type="must")
    #searched = es_obj.getSortedDataByURL(q.query['query'])
#    print(json.dumps(es_obj.search(que, _size=10), ensure_ascii=False, indent=4))


