import tornado, json, sys, logging, traceback
from tornado.httpclient import HTTPClient

url = "http://%s:%s/%s" % ("192.168.9.140", "1111", "query_correct")
http_client = HTTPClient()

def get_res(txt):
    obj = {"header": {},"request": {"c": "", "m": "query_correct", "p": {"query": txt}}}
    response = http_client.fetch(tornado.httpclient.HTTPRequest(
        url=url,
        method="POST",
        headers={'Content-type': 'application/json'},
        body=json.dumps(obj, ensure_ascii=False)
    ))
    result = json.loads(response.buffer.read().decode("utf-8", errors='ignore'))
    return result

def test():
    results = []
    with open("../candidate_query1/querys", encoding="utf8") as f:
        for line in f.readlines():
            line = line.strip().split("@")
            if len(line) != 2 or line[0] != 'query' or len(line[1].split('&')) != 2: continue
            q, f = line[1].split('&')
            try:
                res = get_res(q)['response']['results']['correct_result'][0]
                #print(line[1] + ' -> ' + res[0]+'&'+str(res[1]))
                results.append((line[1] + ' -> ' + res[0]+'&'+str(res[1])))
            except Exception as e:
                print(q, res); continue
    http_client.close()
    with open("./querys_test", 'w', encoding="utf8") as f:
        for e in results:
            f.write(e + '\n')

if __name__ == '__main__':
    test(); exit()
    try: que = sys.argv[1]
    except: que = "百度后端"
    print(json.dumps(get_res(que), ensure_ascii=False))

