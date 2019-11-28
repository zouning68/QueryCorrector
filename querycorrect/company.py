import tornado, sys, json, logging, requests
from seg_utils import regular_cut
from utils import is_name

def is_company(content):
    url = "http://algo.rpc/corp_tag"
    company_id = 0
    try:
        obj = {"header": {},
               "request": {
                "p": {"corp_name": content},
                "c":"corp_tag_simple",
                "m":"corp_tag_by_name"}}
        query = json.dumps(obj)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        response = requests.post(url, data=query, headers=headers)
        response = json.loads(response.text)
        #print(json.dumps(response, ensure_ascii=False, indent=4)); exit()
        result = response['response']['result']
        if result['company_id']: company_id = result['company_id']
        else: company_id = 0
    except Exception as e:
        logging.warning('is_company_error=%s' % (str(repr(e))))
    if company_id > 0: return True
    else: return False

def get_entity(text):
    result = {}; result['entitys'] = []; result['detail'] = []; result['info'] = []
    seg_text = regular_cut(text)
    for word in seg_text:
        idx = text.find(word)
        if 0 and is_company(word):
            result['entitys'].append(word)
            result['info'].append([word, idx, idx + len(word), 'company'])
        elif is_name(word):
            result['entitys'].append(word)
            result['info'].append([word, idx, idx + len(word), 'name'])
    return result

def sort_map(resMap):
    if resMap:
        return sorted(resMap.items(), key=lambda d:d[1], reverse = True)
    else:
        return []

def get_query_entity(original_query, k = '', v = '', human = 'tag'):
    url="http://algo.rpc/query_tagging"
    result = {}; result['entitys'] = []; result['detail'] = []; result['info'] = []
    valid_entitys = ['CORP', 'INDUSTRY', 'FUNCTION', 'NAME', 'SCHOOL']
    try:
        ive_mode = {'pos':'intervene/positive','posa':'intervene/positive/append','neg':'intervene/negative','tag':'','rcorp':'intervene/rewrite/corp', \
                'sflag':'intervene/ switch_flag','title':'intervene/title','qmap':'intervene/query_mapping','rfunc':'intervene/rewrite/func'}
        obj = {"header":{},"request":{"c":"query_tagging","m":ive_mode[human],"p":{"query":original_query,"fmt":1,"k":k,"v":v}}}
        query = json.dumps(obj)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        response = requests.post(url, data=query, headers=headers)
        response = json.loads(response.text)
        query_tags = response['response']['results']['query_tags']
        #print(json.dumps(query_tags, ensure_ascii=False, indent=4)); exit()
        for k, v in query_tags.items():
            ent = k.split(':')[1]
            idx = original_query.find(ent)
            sort_result = sort_map(v)
            if sort_result and sort_result[0][0] in valid_entitys and sort_result[0][1] > 0.5:
                result['detail'].append((ent, sort_result[0][0], sort_result[0][1]))
                result['entitys'].append(ent)
                result['info'].append([ent, idx, idx + len(ent), 'entity'])
    except Exception as e:
        logging.warning('get_query_tag_error=%s' % (str(repr(e))))
    return result

if __name__ == '__main__':
    try: corp = sys.argv[1]
    except: corp = '百度公司'
    print(json.dumps(get_entity(corp), ensure_ascii=False, indent=4))
    #print(json.dumps(is_company(corp), ensure_ascii=False, indent=4))
    #print(json.dumps(get_query_entity(corp), ensure_ascii=False, indent=4))



