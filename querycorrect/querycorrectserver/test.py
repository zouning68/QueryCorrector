import tornado, json, sys, logging, traceback, progressbar, Levenshtein, re, random
from tornado.httpclient import HTTPClient
from tqdm import tqdm
import numpy as np
from config import config
from correct import Corrector
from collections import defaultdict
#from spider import spider

url = "http://%s:%s/%s" % ("192.168.9.140", "1111", "query_correct"); test_file = "querys_test"
url = "http://%s:%s/%s" % ("127.0.0.1", "51668", "query_correct"); test_file = "querys_test1"
http_client = HTTPClient()
#qc = Corrector()

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
    with open(path, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(path, encoding="utf8") as fin:
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            match_res = matchObj.match(line)
            if not match_res: continue
            q, f = match_res.group(1), int(match_res.group(2))
            #correct, detail = spider(q)
            correct, detail = get_res(q)
            #correct, detail = qc.correct(q)
            # print(correct, detail); exit()
            if q == correct or not detail: continue
            results.append(q + ' -> ' + correct)
    if 1:
        random.seed(1)
        for e in t:
            correct, detail = get_res(e)
            #correct, detail = qc.correct(e)
            if not (e == correct or not detail): results.append(e + ' -> ' + correct)
    #    random.shuffle(results)
    http_client.close()
    with open(test_file, 'w', encoding="utf8") as fin:
        for e in results:
            fin.write(e + '\n')

def test_jdtitle():
    results = []    ;random.seed(1)
    jdtitles = [line for line in open(config.jd_title, encoding='utf8').readlines() if matchObj.match(line) and int(matchObj.match(line).group(2)) == 1]
    random.shuffle(jdtitles); jdtitle = jdtitles[: 10000]; print("\noriginal data: %d\tsample data: %d" % (len(jdtitles), len(jdtitle)))
    for i, line in enumerate(tqdm(jdtitle, total=len(jdtitle))):
        match_res = matchObj.match(line)
        q, f = match_res.group(1), int(match_res.group(2))
        correct, detail = get_res(q)
        #correct, detail = qc.correct(q)
        if q == correct or not detail: continue
        results.append(q + ' -> ' + correct)
    http_client.close()
    with open("./jdtitle_test", 'w', encoding="utf8") as fin:
        for e in results:
            fin.write(e + '\n')

t=['puthon','jvav架构师','puthon开法','开发工成师','appl官王','行政专远','人力资源找聘','美团,数局挖掘','百读,产品经理','大数据开法,jaca','hadop开发,北京,本科',\
   '小洪书,java工成师','andorid开法','gloang']
def test():
    for q in t:
        r = get_res(q)['response']['results']['correct_result']
        print(q, r)
    http_client.close()

if __name__ == '__main__':
    try: que = sys.argv[1]
    except: que = "montage+深圳"
    #print(json.dumps(get_res(que), ensure_ascii=False))
    #test(); exit()
    test_querys(); exit()
    test_jdtitle()
