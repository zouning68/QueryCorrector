import Levenshtein, json, re, logging, traceback
import numpy as np
from collections import Counter

Ngram = 4
CandidateQueryFile = "./data/candidate_query"
NGramFile = "./data/ngrams"

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

def n_gram_words(text, n_gram, return_list=False):
    # n_gram 句子的词频字典
    words, words_freq = [], dict()
    try:
        # 1 - n gram
        for i in range(1, n_gram + 1):
            words += [text[j: j + i] for j in range(len(text) - i + 1)]
        words_freq = dict(Counter(words))
    except Exception as e:
        logging.warning('n_gram_words_err=%s' % repr(e)); print(traceback.format_exc())
    if return_list: return words
    else: return words_freq

def rmPunct(line):
    line = re.sub(r"[★\n-•／［…\t」＋＆　➕＊]+", "", line)
    line = re.sub(r"[,\./;'\[\]`!@#$%\^&\*\(\)=\+<> \?:\"\{\}-]+", "", line)
    line = re.sub(r"[、\|，。《》；“”‘’；【】￥！？（）： ～]+", "", line)
    line = re.sub(r"[~/'\"\(\)\^\.\*\[\]\?\\]+", "", line)
    return line

a=rmPunct("ｃｐ　软")

def clean_query(query):
    query = query.strip().lower()
    query = rmPunct(query)
    return query

def read_file(file_path):
    res = {}
    with open(file_path, encoding='utf8') as f:
        for line in f.readlines():
            line = line.strip()
            line_seg = line.split('&')
            if len(line_seg) != 2: continue
            try:
                k, v = line_seg[0], int(line_seg[1])
                res[k] = v
            except Exception as e:
                logging.warning('read_file_err=%s' % repr(e)); print(traceback.format_exc())
    return res

def nomal_qeury(text):
    re_ch = re.compile("([\u4e00-\u9fa5])", re.S)
    re_digital = re.compile("[0-9]{3,}", re.S)
    digital = re_digital.findall(text)
    chinese = re_ch.findall(text)
    if not chinese and digital or len(text) > 20 or len(text) < 3: return False
    else: return True

def get_info(origi_dict, key, *args, flag=False):
    res = []
    try:
        if key not in origi_dict or type(origi_dict[key]) != type({}):
            return res
        info = origi_dict[key]
        if flag:
            for arg in args[0]:
                if arg in info and info[arg] and isinstance(info[arg], str):
                    res.append(key+'_'+arg+'@'+info[arg])
        else:
            for k, v in info.items():
                for arg in args[0]:
                    if arg in v and v[arg] and isinstance(v[arg], str):
                        res.append(key+'_'+arg+'@'+v[arg])
    except Exception as e:
        logging.warning('get_info_err=%s' % repr(e)); print(traceback.format_exc())
    return res

def resolve_dict(dict_info):
    res = []
    try:
        if not isinstance(dict_info, dict): return res
        for k, v in dict_info.items():
            edu = get_info(v, 'education', ['discipline_name', 'school_name'])
            work = get_info(v, 'work', ['corporation_name', 'title_name', 'industry_name', 'position_name'])
            certificate = get_info(v, 'certificate', ['name'])
            project = get_info(v, 'project', ['corporation_name', 'name'])
            skill = get_info(v, 'skill', ['name'])
            basic = get_info(v, 'basic', ['expect_position_name', 'not_expect_corporation_name', 'title_name'], flag=True)
            res.extend(edu); res.extend(work); res.extend(certificate); res.extend(project); res.extend(skill); res.extend(basic)
    except Exception as e:
        logging.warning('resolve_dict_err=%s' % repr(e)); print(traceback.format_exc())
    return res

def parse_line(line):
    querys, tmp = [], []
    try:
        line = line.strip().lower().split('\t')
        if len(line) >= 5:
            tmp.append('query@'+line[5])
        if len(line) >= 36:
            cv_info = json.loads(line[36])
            tmp.extend(resolve_dict(cv_info))
        querys = tmp
        '''
        for q in tmp:
            q = clean_query(q)
            if nomal_qeury(q):
            querys.append(q)
        '''
    except Exception as e:
        logging.warning('parse_line_err=%s' % repr(e)); print(traceback.format_exc())
    return querys

def parse_line_ngrams(line):
    ngrams = []
    try:
        querys = parse_line(line)
        for q in querys:
            if not isinstance(q, str): continue
            ngrams.extend(n_gram_words(q, 4, True))
    except Exception as e:
        logging.warning('parse_line_ngrams_err=%s' % repr(e)); print(traceback.format_exc())
    return ngrams

def parse_line_querys(line):
    querys = []
    try:
        querys = parse_line(line)
    except Exception as e:
        logging.warning('parse_line_querys_err=%s' % repr(e)); print(traceback.format_exc())
    return querys

def test():
    #read_file(CandidateQueryFile)
    res = {}
    with open("../code_back/querys", encoding="utf8") as f:
        for line in f.readlines():
            line = line.strip().split("@")
            if len(line) != 2: continue
            if line[0] not in res: res[line[0]] = []
            res[line[0]].append(line[1])
    for k, v in res.items():
        v_map = {e.split('&')[0]: int(e.split('&')[1]) for e in v if len(e.split('&')) == 2 and e.split('&')[1].isdigit()}
        v_sorted = sorted(v_map.items(), key = lambda d: d[1], reverse=True)
        vs = [e[0]+'&'+str(e[1]) for e in v_sorted]
        #print(k,v,v_map,v_sorted,vs); exit()
        with open("../code_back/query_static/"+str(k), 'w', encoding="utf8") as f:
            for e in vs:
                f.write(e+'\n')
    #print(res)
    exit()
    txt = open("../query_correct_0/data/search_data.log1", encoding="utf8").readlines()
    ngrams, querys = [], []
    for line in txt:
        #ngrams.extend(parse_line_ngrams(line))
        querys.extend(parse_line_querys(line))
    a=1

def aa():
    import time, jieba
    import progressbar
    a = time.strftime('%Y-%m-%d_%H_%M_%S',time.localtime(time.time()))
    aa=list(jieba.cut("京东金融,秦京"))
    aaa=list(jieba.tokenize("京东金融"))

    for i in progressbar.progressbar(100):
        time.sleep(0.02)

if __name__ == '__main__':
    aa(); exit()
    print(abs(len("房地产/建筑/建材/工程") - len("房地产,/建筑/建材/工程")) < 1);exit()
    test(); exit()
    a = nomal_qeury("k12d2d2")
    print(read_file(CandidateQueryFile)); exit()
    exit()
    s1, s2 = '人工智能行业', '智能人工'
    print('%s\t%s' % (edit_distance(s1, s2), Levenshtein.distance(s1, s2)))
    pass


