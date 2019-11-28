import requests, urllib, time, sys, re
from lxml import etree
from bs4 import BeautifulSoup

xpath = '//*[@id="super_se_tip"]/div/span/strong[2]'
xpath = '/html/body/div[1]/div[3]/div[1]/div[3]/div[1]/div/span/strong[2]'
url1 = "&rsv_spt=1&rsv_iqid=0x8412861f00002146&issp=1&f=8&rsv_bp=1&rsv_idx=2&ie=utf-8&rqlang=cn&tn=baiduhome_pg&rsv_enter=1&rsv_dl=tb"
url2 = "&oq=e%25E6%2589%25AB%25E5%25AE%259D&inputT=2917&rsv_t=9f38QZlaR0Nomp87tXyY75v%2BC40NFgfMO8Fx0TfB8Ug4llrWBQeVXLMVX%2Bj5vgw6cIH3"
url3 = "&rsv_pq=d26af51500029583&rsv_sug3=30&rsv_sug1=10&rsv_sug7=100&rsv_sug2=0&rsv_sug4=2917"
u = 'https://www.baidu.com/s?wd=%E5%BC%80%E5%8F%91%E5%B7%A5%E7%A8%8B%E5%B8%88&rsv_spt=1&rsv_iqid=0xc5d1b7f1000007e5&issp=1&f=8&rsv_bp=1&rsv_idx=2&ie=utf-8&tn=baiduhome_pg&rsv_enter=1&rsv_dl=tb&rsv_sug3=10&rsv_sug1=4&rsv_sug7=100&rsv_sug2=0&inputT=2576&rsv_sug4=2576'
def spider1(query):
    try:
        #time.sleep(2)
        url = "https://www.baidu.com/s?wd=" + urllib.parse.quote(query) + url1 + url2 + url3
        resp = requests.get(url)
        resp.encoding = 'utf-8'
        html = resp.text
        if " <title>百度安全验证</title>" in html:
            status = 'fail'
        else:
            status = 'success'
        root = etree.HTML(html)
        # 获取某个tr节点下面的所有文本数据数据
        tr_text_all = root.xpath(xpath)
        if tr_text_all:
            correct_query = tr_text_all[0].text
        else:
            correct_query = query
        return correct_query, 'success'
    except Exception as e:
        print('spider_error=%s' % (str(repr(e))))
        return query, ''

s = requests.session()
s.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',}
matchObj = re.compile(r'已显示“(.+)”的搜索结果.*', re.M | re.I)
def spider(query):
    correct_query = query
    url = "https://www.baidu.com/s?wd=" + urllib.parse.quote(query) + url1 + url2 + url3
    #res = requests.get(url)
    while True:
        time.sleep(0.002)
        try:
            res = s.get(url)
            res.encoding = 'utf-8'
            if " <title>百度安全验证</title>" in res.text: status = False
            else: status = True
            soup = BeautifulSoup(res.text, 'lxml')
            hit_top_new = soup.find('div', class_='hit_top_new')
            if hit_top_new:
                matchres = matchObj.search(hit_top_new.text)
                if matchres:
                    correct_query = matchres.group(1)
            if status:
                break
        except Exception as e:
            status = False
            print("spider error" + str(e))
    return correct_query, 'success'

if __name__ == '__main__':
    try: que = sys.argv[1]
    except: que = "开法工程师"
    #print(spider1(que))
    print(spider(que))
