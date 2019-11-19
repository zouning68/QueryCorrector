import tornado, json, sys, logging, traceback, progressbar, Levenshtein
from tornado.httpclient import HTTPClient
from tqdm import tqdm
import numpy as np
from config import config
from correct import Corrector

url = "http://%s:%s/%s" % ("192.168.9.140", "1111", "query_correct")
http_client = HTTPClient()

def test_levenshtein():
    texta = 'kitten'    #'艾伦 图灵传'
    textb = 'sitting'    #'艾伦•图灵传'
    print(Levenshtein.distance(texta,textb))        # 计算编辑距离
    print(Levenshtein.hamming(texta,textb))  # 计算汉明距离
    print(Levenshtein.ratio(texta,textb))           # 计算莱文斯坦比
    print(Levenshtein.jaro(texta,textb))            # 计算jaro距离
    print(Levenshtein.jaro_winkler(texta,textb))    # 计算Jaro–Winkler距离
    print(Levenshtein.distance(texta,textb))

def edit_distance(word1, word2):
    len1, len2 = len(word1), len(word2)
    dp = np.zeros((len1 + 1, len2 + 1))
    for i in range(len1 + 1):
        dp[i][0] = i
    for j in range(len2 + 1):
        dp[0][j] = j

    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            delta = 0 if word1[i - 1] == word2[j - 1] else 1
            dp[i][j] = min(dp[i - 1][j - 1] + delta, min(dp[i - 1][j] + 1, dp[i][j - 1] + 1))
    return dp[len1][len2]

def get_res(txt):
    obj = {"header": {},"request": {"c": "", "m": "query_correct", "p": {"query": txt}}}
    response = http_client.fetch(tornado.httpclient.HTTPRequest(
        url=url,
        method="POST",
        headers={'Content-type': 'application/json'},
        body=json.dumps(obj, ensure_ascii=False)
    ))
    result = json.loads(response.buffer.read().decode("utf-8", errors='ignore'))
    res = result['response']['results']['correct_result']
    return res[0][0], res[1]

def get_query_tag(query, k = '', v = '', human = 'tag'):
    ive_mode = {'pos':'intervene/positive','posa':'intervene/positive/append','neg':'intervene/negative','tag':'','rcorp':'intervene/rewrite/corp','sflag':'intervene/switch_flag','title':'intervene/title','qmap':'intervene/query_mapping','rfunc':'intervene/rewrite/func'}
    obj = {"header":{},"request":{"c":"query_tagging","m":ive_mode[human],"p":{"query":query,"fmt":1,"k":k,"v":v}}}
    response = http_client.fetch(tornado.httpclient.HTTPRequest(
        url="http://algo.rpc/query_tagging",
        method="POST",
        headers={'Content-type': 'application/json'},
        body=json.dumps(obj, ensure_ascii=False)
    ))
    result = json.loads(response.buffer.read().decode("utf-8", errors='ignore'))
    http_client.close()
    return result

def test_querys():
    qc = Corrector()
    results = []
    with open(config.query, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(config.query, encoding="utf8") as fin:
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            try:
                seg_line = line.strip().split("&")
                if len(seg_line) != 2: continue
                q, f = seg_line
                correct, detail = get_res(q)
                #correct, detail = qc.correct(q)
                #print(correct, detail); exit()
                if q == correct or not detail: continue
                results.append(q + ' -> ' + correct)
            except Exception as e:
                print(q); continue
    http_client.close()
    with open("./querys_test", 'w', encoding="utf8") as f:
        for e in results:
            f.write(e + '\n')

def test():
    for q in ['百度后端','andio','excle','jaca','wold','开发工成师','工程造假','puthon','jvav,8年以上,金融']:
        r = get_res(q)['response']['results']['correct_result']
        print(q, r)
    http_client.close()

if __name__ == '__main__':
    try: que = sys.argv[1]
    except: que = "百度后端"
    #print(json.dumps(get_query_tag(que), ensure_ascii=False, indent=4)); exit()
    #test(); exit()
    test_querys(); exit()
    print(json.dumps(get_res(que), ensure_ascii=False))

