import logging, json, requests, jieba, sys
from nlutools import tools as nlu

def cut(text):
    res = []
    try:
        url = 'http://192.168.12.18:51990/huqie'
        body = {"txt":str(text)}
        query = json.dumps(body)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        response = requests.post(url, data=query, headers=headers)
        response = json.loads(response.text)
        res = response['1']['txt']
    except Exception as e:
        logging.warn('getSegTxt_error=%s' % (str(repr(e))))
    return res

def jieba_cut(text):
    res = list(jieba.cut(text))
    return res

def nlu_cut(text):
    res = nlu.cut(text)
    return res

if __name__ == '__main__':
    try: que = sys.argv[1]
    except: que = "上海大岂网络科技有限公司"
    nlu_seg = nlu_cut(que)
    jieba_seg = jieba_cut(que)
    print(json.dumps(cut(que), ensure_ascii=False))   # 分词服务

