import tornado, sys, json, logging, requests

def is_company(content):
    url = "http://algo.rpc/corp_tag"
    company_id = 0
    try:
        obj = {
                "header": {
                    },
                "request": {
                    "p": {
                        "corp_name": content
                    },
                "c":"corp_tag_simple",
                "m":"corp_tag_by_name"
                }
            }
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

if __name__ == '__main__':
    try: corp = sys.argv[1]
    except: corp = '百度公司'
    print(json.dumps(is_company(corp), ensure_ascii=False, indent=4))



