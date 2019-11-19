import zlib, binascii, json, re, os
from config import config
from data_utils import Tokenizer
from tqdm import tqdm
from collections import defaultdict
from utils import re_en, is_chinese, PUNCTUATION_LIST

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

def handle_corpus():
    cvdata = [line.strip().split('\t') for line in open("corpus/cvdata0", encoding='utf8').readlines()]
    jddata = [line.strip().split('\t') for line in open("corpus/jddata0", encoding='utf8').readlines()]
    jdposition = [line.strip().split('\t') for line in open("corpus/jdposition0", encoding='utf8').readlines()]
    for line in cvdata:
        cv = json.loads(uncompress(line[1]))
        pass

def merge_dict(dict1, dict2, th1=1, th2=1):
    res = defaultdict(int)
    _dict1 = {k: v for k, v in dict1.items() if v > th1}
    _dict2 = {k: v for k, v in dict2.items() if v > th2}
    res.update(_dict1)
    res.update(_dict2)
    return res

def gen_train_data():
    candidate_query, common_char_set, word_freq = defaultdict(int), defaultdict(int), defaultdict(int)
    q_candidate_query, n_candidate_query, q_english, n_english = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
    original_query = defaultdict(int)
    token = Tokenizer()
    print("\nread file: %s" % (config.original_query_path))
    with open(config.original_query_path, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(config.original_query_path, 'r', encoding="utf8") as fin:
        #   query@百度.net开发工程师&3156
        for i, line in enumerate(tqdm(fin, total=num_lines)):
#            if i > 1000: break          #   TEST
            matchObj = re.match(r'([a-zA-Z\_]+)@(.+)&([0-9]+)', line, re.M | re.I)
            if not matchObj: continue
            names, query, freq = matchObj.group(1), matchObj.group(2), int(matchObj.group(3))
            #if names == "query": continue       #  ************** 交叉验证方法 **************
            correct_sentence, senten2term, char_seg, word_seg, detail_eng = token.tokenize(query, False)
            senten2termstr = ' '.join(senten2term)
            if names == "query":
                q_candidate_query[senten2termstr] += freq
                original_query[query] += freq
            else: n_candidate_query[senten2termstr] += freq
            for term in senten2term:        # term 集合
                term = term.strip()         ;a=re.fullmatch(r'([a-zA-Z]{1}|[0-9]{1,2})', term, re.M | re.I)
                if not term or term in PUNCTUATION_LIST or re.fullmatch(r'([a-zA-Z]{1}|[0-9]{1,2})', term, re.M | re.I): continue
                word_freq[term] += freq
                for t in list(term):        # 公共汉字字符处理
                    if is_chinese(t):
                        common_char_set[t] += freq
                if re_en.fullmatch(term) and len(term) > 1:       # 英文集合
                    if names == "query": q_english[term] += freq
                    else: n_english[term] += freq
    _common_char_set = {k: v for k, v in common_char_set.items() if v > 100}
    _candidate_query = merge_dict(q_candidate_query, n_candidate_query, 1, 10)
    _word_freq = {k: v for k, v in word_freq.items() if v > 100}
    _english = merge_dict(q_english, n_english, 1, 100)
    # 加入英文实体集合
    english_tmp = defaultdict(int)
    with open(config.kg_nodes_full_data, encoding='utf8') as fin:
        for line in fin.readlines():
            name = json.loads(line)['name'].lower()
            for word in re_en.split(name):
                if re_en.fullmatch(word):
                    for w in word.split():
                        if len(w) <= 1: continue
                        english_tmp[w] += 10000
    _english.update(english_tmp)
    _english_corpu = {' '.join(k): v for k, v in _english.items()}
    # 排序
    sorted_candidate_query = sorted(_candidate_query.items(), key=lambda d: d[1], reverse=True)
    sorted_common_char_set = sorted(_common_char_set.items(), key=lambda d: d[1], reverse=True)
    sorted_english = sorted(_english.items(), key=lambda d: d[1], reverse=True)
    sorted_word_freq = sorted(_word_freq.items(), key=lambda d: d[1], reverse=True)
    sorted_english_corpu = sorted(_english_corpu.items(), key=lambda d: d[1], reverse=True)
    sorted_original_query = sorted(original_query.items(), key=lambda d: d[1], reverse=True)
    # 写入文件
    write_file(sorted_candidate_query, config.candidate_query_path, "&")        # 候选的query集合
    write_file(sorted_common_char_set, config.common_char_path)                 # 公共汉字符集，构造一级编辑距离的纠错候选集
    write_file(sorted_english, config.english_path)                             # 英语单词集合
    write_file(sorted_word_freq, config.word_freq_path)                         # 词频
    write_file(sorted_original_query, config.query, "&")                        # 原始的query
    # 训练语言模型的语料
    write_file(sorted_candidate_query, config.query_corpus_ch, need_freq=False)
    write_file(sorted_candidate_query[len(sorted_candidate_query) - 1000: ], config.query_corpus_ch+'_test', need_freq=False)
    write_file(sorted_english_corpu, config.query_corpus_en, need_freq=False)
    write_file(sorted_english_corpu[len(sorted_english_corpu) - 1000: ], config.query_corpus_en+'_test', need_freq=False)
    a=1

def write_file(obj, file_path, sep=" ", need_freq=True):
    print("\nwrite file: %s" % (file_path))
    with open(file_path, 'w', encoding="utf8") as fin:
        for k, v in tqdm(obj, total=len(obj)):
            if len(k) == 0: continue
            if need_freq: fin.write(k + sep + str(v) + "\n")
            else: fin.write(k + "\n")

if __name__ == "__main__":
    #handle_corpus()
    gen_train_data()