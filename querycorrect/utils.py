import json, re, logging, traceback, jieba, progressbar, os
from collections import Counter
#from nlutools import tools as nlu
from config import config

#**********************************************************************************************************************#
names = [e.strip() for e in open(config.baijiaxing, encoding='utf8').readlines() if e.strip() != '']
def is_name(text):
    text = str(text)    ; aa=text[0]
    if len(text) > 2 and text[:2] in names: return True
    if len(text) in [1, 2, 3] and text[0] in names: return True
    else: return False
a=is_name("锜晓敏")

PUNCTUATION_LIST = ".。,，,、?？:：;；{}[]【】“‘’”《》/!！%……（）<>@#$~^￥%&*\"\'=+-_——「」"
re_ch = re.compile(u"([\u4e00-\u9fa5])",re.S)
re_en = re.compile(u"([a-zA-Z\+\#]+)",re.S)

def is_alphabet_string(string):     # 判断是否全部为英文字母
    string = string.lower()
    for c in string:
        if c < 'a' or c > 'z':
            return False
    return True
a=is_alphabet_string("Java开")

def Q2B(uchar):     # 全角转半角
    inside_code = ord(uchar)
    if inside_code == 0x3000:
        inside_code = 0x0020
    else:
        inside_code -= 0xfee0
    if inside_code < 0x0020 or inside_code > 0x7e:  # 转完之后不是半角字符返回原来的字符
        return uchar
    return chr(inside_code)

def stringQ2B(ustring):     # 把字符串全角转半角
    return "".join([Q2B(uchar) for uchar in ustring])

def uniform(ustring):       # 格式化字符串，完成全角转半角，大写转小写的工作
    return stringQ2B(ustring).lower()

def is_chinese(uchar):      # 判断一个unicode是否是汉字
    if '\u4e00' <= uchar <= '\u9fa5':
        return True
    else:
        return False

def is_chinese_string(string):      # 判断是否全为汉字
    for c in string:
        if not is_chinese(c):
            return False
    return True

def edit_distance_word(word, char_set):     # all edits that are one edit away from 'word'
    splits = [(word[:i], word[i:]) for i in range(len(word) + 1)]
    transposes = [L + R[1] + R[0] + R[2:] for L, R in splits if len(R) > 1]
    replaces = [L + c + R[1:] for L, R in splits if R for c in char_set]
    return set(transposes + replaces)
#**********************************************************************************************************************#

def n_gram_words(text, n_gram=4, return_list=False):
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

def test():
    txt = open("../query_correct_0/data/search_data.log1", encoding="utf8").readlines()
    ngrams, querys = [], []
    for line in txt:
        querys.extend(parse_line_querys(line))
        ngrams.extend(parse_line_ngrams(line))
    a=1

if __name__ == '__main__':
    file_name = "../candidate_query_2019-11-07_18_42_01/querys"
#    resolv_querys("./data/querys1", 10, './data/q', './data/n'); exit()  # 构建query数据
    #resolv_querys(file_name, 10); exit()     # 构建query数据
    test(); exit()
    a = normal_qeury("k12d2d2")
