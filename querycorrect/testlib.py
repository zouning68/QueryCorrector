import tornado, json, sys, logging, traceback, progressbar, re, random, codecs
from tornado.httpclient import HTTPClient
from tqdm import tqdm
import numpy as np
#from querycorrectlib2.qc import queryCorrect        # python2
#from querycorrectlib3.qc import queryCorrect        # python3
from correct import Corrector

url = "http://%s:%s/%s" % ("192.168.9.140", "51668", "query_correct")
url = "http://%s:%s/%s" % ("192.168.7.205", "51668", "query_correct")
#http_client = HTTPClient()
qc = Corrector()

t=['puthon','jvav架构师','puthon开法','开发工成师','appl官王','行政专远','人力资源找聘','美团,数局挖掘','百读,产品经理','大数据开法,jaca','hadop开发,北京,本科',\
   '小洪书,jav工成师','andorid开法','gloang']

def get_res(txt):
    try:
        obj = {"header": {},"request": {"c": "", "m": "query_correct", "p": {"query": txt}}}
        response = http_client.fetch(tornado.httpclient.HTTPRequest(
            url=url,
            method="POST",
            headers={'Content-type': 'application/json'},
            body=json.dumps(obj, ensure_ascii=False)
        ))
        result = json.loads(response.buffer.read().decode("utf-8", errors='ignore'))
        res = result['response']['results']
    except Exception as e:
        print(txt)
    return res['corrected_query'], res['detail']

matchObj = re.compile(r'(.+)&([0-9]+)', re.M | re.I)
def test_querys():
    results = []; path = 'query_original'
    with codecs.open(path, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with codecs.open(path, encoding="utf8") as fin:
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            detail = [1]
            match_res = matchObj.match(line)
            if not match_res: continue
            q, f = match_res.group(1), int(match_res.group(2))
            #correct, detail = get_res(q)
            correct = qc.correc(q)
            # print(correct, detail); exit()
            if q == correct or not detail: continue
            results.append(q + ' -> ' + correct)
    if 1:
        random.seed(1)
        for q in t:
            detail = [1]
            #correct, detail = get_res(q)
            correct = qc.correc(q)
            if q == correct or not detail: continue
            results.append(q + ' -> ' + correct)
    #    random.shuffle(results)
    http_client.close()
    with codecs.open('qctest2', 'w', encoding="utf8") as fin:
        for e in results:
            fin.write(e + '\n')

def test_search_querys():
    matchObj = re.compile(r'(.+)\t ([0-9]+)', re.M | re.I)
    results = []; path = 'corpus/sort_search_data'
    with codecs.open(path, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with codecs.open(path, encoding="utf8") as fin:
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            detail = [1]    #; line = "，	 1"
            match_res = matchObj.match(line)
            if not match_res: continue
            q, f = match_res.group(1), int(match_res.group(2))
            #correct, detail = get_res(q)
            try: correct, detail = qc.correct(q)
            except: print(i, line)
            #print(correct, detail); exit()
            if not detail: continue
            results.append(q + ' -> ' + correct)
    with codecs.open('sort_search_data.test', 'w', encoding="utf8") as fin:
        for e in results:
            fin.write(e + '\n')

def static_result():
    path = "sort_search_data.test"
    lable = {line.strip().split(" -> ")[0]: line.strip().split(" -> ")[1] for line in open("sort_search_data.test.true", encoding="utf8").readlines()}
    pred = {line.strip().split(" -> ")[0]: line.strip().split(" -> ")[1] for line in open(path, encoding="utf8").readlines()}
    TP= 0
    for original, correct in pred.items():
        if original in lable and correct == lable[original]:
            TP += 1
    precision = TP / len(pred)
    recall = TP / len(lable)
    f1 = (2 * precision * recall) / (precision + recall)
    print("precision: %f\trecall: %f\tf1: %f" %(precision, recall, f1))
    aa=1

if __name__ == '__main__':
    try: que = sys.argv[1]
    except: que = "montage+深圳"
    #print(json.dumps(get_res(que), ensure_ascii=False))
    #test_querys()
    #test_search_querys()
    static_result()
