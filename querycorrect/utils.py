import Levenshtein, json, re, logging, traceback, jieba, progressbar, os
import numpy as np
from collections import Counter
#from nlutools import tools as nlu

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

PUNCTUATION_LIST = ".。,，,、?？:：;；{}[]【】“‘’”《》/!！%……（）<>@#$~^￥%&*\"\'=+-_——「」"
re_ch = re.compile(u"([\u4e00-\u9fa5])",re.S)
re_en = re.compile(u"([a-zA-Z]*)",re.S)
a=re_en.split("百度 java 开发工程师 后台开法")
class Tokenizer(object):
    def __init__(self, dict_path='', custom_word_freq_dict=None, custom_confusion_dict=None):
        self.model = jieba
        self.model.default_logger.setLevel(logging.ERROR)
        # 初始化大词典
        if os.path.exists(dict_path):
            self.model.set_dictionary(dict_path)
        # 加载用户自定义词典
        if custom_word_freq_dict:
            for w, f in custom_word_freq_dict.items():
                self.model.add_word(w, freq=f)
        # 加载混淆集词典
        if custom_confusion_dict:
            for k, word in custom_confusion_dict.items():
                # 添加到分词器的自定义词典中
                self.model.add_word(k)
                self.model.add_word(word)

    def tokenize(self, sentence):
        seg_res, cur_index = [], 0    ; sentence = "百度 java 开发工程师 后台开法"; a=re_en.split(sentence);  aa=list(self.model.tokenize(sentence))
        for word in re_en.split(sentence):
            word = word.strip()
            if word in ['', ' ']: continue
            if re_en.fullmatch(word):   # 英文处理
                seg_res.append((word, cur_index, cur_index+len(word)))
            else:                       # 非英文处理
                model_seg = list(self.model.tokenize(word))
                seg_res.extend([(e[0], e[1]+cur_index, e[2]+cur_index) for e in model_seg])
            cur_index = seg_res[-1][2]
        return seg_res
t = Tokenizer(); a = t.tokenize("aaa")

def rmPunct(line):
    line = re.sub(r"[★\n-•／［…\t」＋＆　➕＊]+", "", line)
    line = re.sub(r"[,\./;'\[\]`!@#$%\^&\*\(\)=\+<> \?:\"\{\}-]+", "", line)
    line = re.sub(r"[、\|，。《》；“”‘’；【】￥！？（）： ～]+", "", line)
    line = re.sub(r"[~/'\"\(\)\^\.\*\[\]\?\\]+", "", line)
    return line

a=rmPunct("消费者/顾客word、excel、ppt、visio、xmind")

def clean_query(query):
    query = re.sub(r"[\\/、， ]+", ",", query)
    query = re.sub(r"[（]+", "(", query)
    query = re.sub(r"[）]+", ")", query)
    query = re.sub(r"[【】●|“”]+", " ", query)
    query = re.sub(r"[：]+", ":", query)
    query = re.sub(r"[ ~]+", " ", query)
    query = query.lstrip(",")
    query = query.rstrip(",")
    query = query.strip().lower()
    return query

aa=clean_query("java-eam【maximo】●●●●项目经理、")

def normal_qeury(text):
    re_ch = re.compile("([\u4e00-\u9fa5])", re.S)
    re_digital = re.compile("[0-9]{2,}", re.S)
    re_longdigital = re.compile("[0-9]{5,}", re.S)
    re_valid = re.compile("[简历]", re.S)
    digital = re_digital.findall(text)
    chinese = re_ch.findall(text)
    valid = re_valid.findall(text)
    long_digital = re_longdigital.findall(text)
    if long_digital: return False
    elif digital and not chinese: return False
    elif len(text) > 20 or len(text) < 2 or valid: return False
    else: return True

a=normal_qeury(clean_query("搜狐畅游17173"))

names = [e.strip() for e in open('./data/names', encoding='utf8').readlines() if e.strip() != '']
def is_name(text):
    text = str(text)
    if len(text) in [1, 2, 3] and text[0] in names: return True
    else: return False
aaa=is_name("贺珊")

rech = re.compile(u"([\u4e00-\u9fa5])",re.S)
def is_ch(w):
    w = re.sub(r"[ ,/、，；;。.]+", "", w)
    len_original, len_re = len(w), len(rech.findall(w))
    if len_original == len_re: return True
    else: return False

def is_en(keyword):
    return all(ord(c) < 128 for c in keyword)

aaaa=is_ch("百,读")
aaaaa=is_en("百度")

def read_file(file_path):
    res = {}
    with open(file_path, encoding='utf8') as f:
        for line in f.readlines():
            line = line.strip()
            line_seg = line.split('&')
            if len(line_seg) != 2 or not line_seg[0]: continue
            try:
                k, v = line_seg[0], int(line_seg[1])
                res[k] = v
            except Exception as e:
                logging.warning('read_file_err=%s' % repr(e)); print(traceback.format_exc())
    return res

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
            work = get_info(v, 'work', ['corporation_name', 'title_name', 'industry_name', 'position_name', 'station_name', 'architecture_name', 'city'])
            certificate = get_info(v, 'certificate', ['name'])
            project = get_info(v, 'project', ['name'])
            language = get_info(v, 'certificate', ['name'])
            skill = get_info(v, 'skill', ['name'])
            basic = get_info(v, 'basic', ['expect_position_name', 'expect_industry_name', 'resume_name'], flag=True)
            res.extend(edu); res.extend(work); res.extend(certificate); res.extend(project); res.extend(skill); res.extend(basic)
            res.extend(language)
    except Exception as e:
        logging.warning('resolve_dict_err=%s' % repr(e)); print(traceback.format_exc())
    return res

def parse_line(line):
    querys, tmp = [], []
    try:
        line = line.strip().lower().split('\t')
        if len(line) >= 5:
            tmp.append("query@"+line[5])
        if len(line) >= 36:
            cv_info = json.loads(line[36])
            tmp.extend(resolve_dict(cv_info))
        #querys = tmp
        #'''
        for q in tmp:
            try: na, qu = q.split('@')
            except: continue
            qu = clean_query(qu)
            if not normal_qeury(qu) or is_name(qu): continue
            querys.append(na+'@'+qu)
            seg_query = [e for e in list(jieba.cut(qu)) if len(e) > 2 if e != qu]
            for e in seg_query:
                if is_name(e) or not normal_qeury(e): continue
                querys.append(na + '_seg' + '@' + e)
                a=1
        #'''
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

def filter(Q, query_freq, edit_dist_th=0.8, freq_th=1000):
    query_dist, sorted_query_dist = {}, []  ; a=query_freq.get(Q, 0); aa=is_en(Q)
    if int(query_freq.get(Q, 0)) > 15 or not is_en(Q):
        return False
    for q, f in query_freq.items():
        if q != Q and len(q) == len(Q) and Levenshtein.ratio(q, Q) > edit_dist_th:
            query_dist[q] = int(query_freq.get(q, 0))
    sorted_query_dist = sorted(query_dist.items(), key=lambda d: d[1], reverse=True)
    if sorted_query_dist and sorted_query_dist[0][1] > freq_th and query_freq.get(Q, 0) < sorted_query_dist[0][1]:
        print(Q+'->'+sorted_query_dist[0][0])
        return True
    else:
        return False

def valid_qeury_freq(line):
    q, f, invalid = '', '0', False
    line = line.strip().split("@")
    if len(line) != 2:
        return '', '0', True
    query_freq = line[1].split('&')
    if len(query_freq) != 2 or not query_freq[1].isdigit():
        return '', '0', True
    if line[0] == 'query':
        invalid = True
    if line[0] in ['basic_resume_name']:
        pass
    q, f = query_freq[0], query_freq[1]
    return q, f, invalid

def resolv_querys(file_path, freq_threshold=10, candidate_path=CandidateQueryFile, ngram_path=NGramFile):
    print("input file: %s\nfreq threshold: %d\ncandidate query file: %s\nngram file: %s" % (file_path, freq_threshold, candidate_path, ngram_path))
    candidate_query, ngram_query = [], []
    try:
        candidate, origion_querys = {}, []
        # ********** 读取统计文件得到query和频率 **********
        with open(file_path, encoding="utf8") as f:
            lines = f.readlines()
            print("total lines: %d" % (len(lines)))
            bar = progressbar.ProgressBar()
            for i in bar(range(len(lines))):
            #for i in range(len(lines)):
                #print("total lines: %d\tcurrent line: %d" % (len(lines), i +1), end='\r')
                line = lines[i]
                if line.split('@')[0] == 'query':
                    origion_querys.append(line.strip())
                    continue
                # 原始的query
                q, f, invalid = valid_qeury_freq(line)
                q = clean_query(q)
                if invalid or not normal_qeury(q): continue
                if q not in candidate: candidate[q] = 0
                candidate[q] += int(f)
                # 扩充query集合
                seg_query = []#[e for e in list(jieba.cut(q)) if len(e) > 2]
                a=1; #print(seg_query); exit()
                for e in seg_query:
                    if is_name(e) or not normal_qeury(e): continue     # 过滤掉姓名，过滤掉无效的query
                    if e not in candidate: candidate[e] = 0
                    candidate[e] += 1
        candidate_top = {k: v for k, v in candidate.items() if v > freq_threshold}      # 频率过滤
        print("original querys: %d\ttop querys: %d" % (len(candidate), len(candidate_top)))
        for e in origion_querys:        # 处理原始的query
            try:
                q, f = e.split('@')[1].split('&')
                q = clean_query(q)
                if is_name(q): continue
            except:
                continue
            if normal_qeury(q) and int(f) > 3:
                if q not in candidate_top: candidate_top[q] = 0
                candidate_top[q] += int(f)
        # ********** 清洗query和过滤频率过低的query **********
        query_result = []
        candidate_sorted = sorted(candidate_top.items(), key=lambda d: d[1], reverse=True)      # 根据频率排序
        for query, freq in candidate_sorted:
            query = clean_query(query)
            if not normal_qeury(query): continue
            query_result.append((query, freq))
        # ********** 得到最终的query和ngrams集合 **********
        print("final query: %d" % (len(query_result)))
        ngrams = {}
        for e in query_result:
            candidate_query.append(e[0] + '&' + str(e[1]))
            for w in n_gram_words(e[0], 4, True):
                if w not in ngrams: ngrams[w] = 0
                ngrams[w] += 1
        for k, v in ngrams.items():
            ngram_query.append(k + '&' + str(v))
        # ********** 写入文件 **********
        with open(candidate_path, 'w', encoding='utf8') as f:
            for e in candidate_query: f.write(e + '\n')
        with open(ngram_path, 'w', encoding='utf8') as f:
            for e in ngram_query: f.write(e + '\n')
    except Exception as e:
        logging.warning('resolv_querys_err=%s' % repr(e)); print(traceback.format_exc())

def test():
    #read_file(CandidateQueryFile)
    txt = open("../query_correct_0/data/search_data.log1", encoding="utf8").readlines()
    ngrams, querys = [], []
    for line in txt:
        ngrams.extend(parse_line_ngrams(line))
        querys.extend(parse_line_querys(line))
    a=1

if __name__ == '__main__':
    file_name = "../candidate_query_2019-11-07_18_42_01/querys"
#    resolv_querys("./data/querys1", 10, './data/q', './data/n'); exit()  # 构建query数据
    resolv_querys(file_name, 10); exit()     # 构建query数据
    test(); exit()
    a = normal_qeury("k12d2d2")
    print(read_file(CandidateQueryFile)); exit()
    exit()
    s1, s2 = '人工智能行业', '智能人工'
    print('%s\t%s' % (edit_distance(s1, s2), Levenshtein.distance(s1, s2)))
    pass


