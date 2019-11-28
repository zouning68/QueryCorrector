import json, re, logging, traceback, jieba, zlib, binascii, math
import numpy as np
from utils import clean_query, is_name, normal_qeury, n_gram_words
from seg_utils import Tokenizer

t = Tokenizer()

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
            qu = query2term(qu)
            querys.append(na+'@'+qu)
            seg_query = []  #[e for e in list(jieba.cut(qu)) if len(e) > 2 if e != qu]
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

regex = re.compile(r'电话|微信|办公地址|地址|官方|网站|岗位|任职|福利|待遇|工作时间|理由|联系|职责|要求|薪酬|薪资')
mo1 = regex.findall('联系电话就是微信号')
def parse_line_jd(line):
    res, tmp = [], []
    try:
        seg_line = line.strip().split('\t')
        if len(seg_line) >= 33:
            tmp.extend([line for line in seg_line[33].split('\\n') if not regex.search(line) and normal_qeury(line) and len(line) > 5])
    except Exception as e:
        logging.warning('parse_line_querys_err=%s' % repr(e)); print(traceback.format_exc())
    if len(tmp) < 5 or len(tmp) > 10: return res
    sample_index = set(np.random.randint(low=len(tmp), size=math.ceil(len(tmp) * 0.5)))
    res = [tmp[i] for i in sample_index]
    return res

def query2term(query):
    correct_sentence, senten2term, char_seg, word_seg, detail_eng = t.tokenize(query, False)
    return ' '.join(senten2term)

def parse_line_jdtitle(line):
    res= []
    try:
        seg_line = line.strip().split('\t')
        if len(seg_line) == 24 and normal_qeury(seg_line[3]):
            query = query2term(clean_query(seg_line[3]))
            res.append(query)
    except Exception as e:
        logging.warning('parse_line_jdtitle_err=%s' % repr(e))
    return res

def uncompress(s):
    try:
        txt = zlib.decompress(binascii.unhexlify(s))
    except TypeError as e:
        txt = "{}"
    try:
        json_obj = json.loads(txt, strict=False)
        txt = json.dumps(json_obj, ensure_ascii=False)
        if (json_obj == None):
            txt = "{}"
    except ValueError as e:
        txt = "{}"
    return txt

def parse_line_cv(line):
    res = []
    try:
        seg_line = line.strip().split('\t')
        cv_info = json.loads(uncompress(seg_line[1]))
        for k, v in cv_info['work'].items():
            responsibility = v['responsibilities']
            res.append(responsibility)
    except Exception as e:
        logging.warning('parse_line_querys_err=%s' % repr(e)); print(traceback.format_exc())
    return res

def test():
    txtjd = open("corpus/jdposition0", encoding="utf8").readlines()
    for line in txtjd: parse_line_jdtitle(line)
    txtcv = open("corpus/cvdata0", encoding="utf8").readlines()
    for line in txtcv: parse_line_cv(line)
    txt = open("corpus/search_data.log", encoding="utf8").readlines()
    ngrams, querys = [], []
    for line in txt:
        querys.extend(parse_line_querys(line))
        ngrams.extend(parse_line_ngrams(line))
    a=1

if __name__ == '__main__':
    test()
