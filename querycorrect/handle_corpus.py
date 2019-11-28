import json, re, os
from config import config
from seg_utils import Tokenizer
from tqdm import tqdm
from collections import defaultdict
from utils import re_en, is_chinese, PUNCTUATION_LIST, clean_query, pinyin2hanzi
from parse_utils import uncompress

def handle_corpus():
    cvdata = [line.strip().split('\t') for line in open("corpus/cvdata0", encoding='utf8').readlines()]
    jddata = [line.strip().split('\t') for line in open("corpus/jddata0", encoding='utf8').readlines()]
    jdposition = [line.strip().split('\t') for line in open("corpus/jdposition0", encoding='utf8').readlines()]
    for line in cvdata:
        cv = json.loads(uncompress(line[1]))
        pass

def merge_dict(dict1, dict2, th1=0, th2=0):
    res = defaultdict(int)
    _dict1 = {k: v for k, v in dict1.items() if v > th1}
    _dict2 = {k: v for k, v in dict2.items() if v > th2}
    res.update(_dict1)
    res.update(_dict2)
    return res

matchObj = re.compile(r'([a-zA-Z\_]+)@(.+)&([0-9]+)', re.M | re.I)
matchObj1 = re.compile(r'(.+)&([0-9]+)', re.M | re.I)
filter_names = ['work_architecture_name1', 'project_name1']
def load_corpu_file():
    normalQuery, expandQuery, jdTitle, skillEntity = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
    # **************************************** query和cv的信息 ****************************************************
    print("\nread file: %s" % (config.original_query_path))
    with open(config.original_query_path, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(config.original_query_path, 'r', encoding="utf8") as fin:
        #   query@百度.net开发工程师&3156
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            line = line.strip().lower()
            if TEST and i > 10: break          #   TEST
            match_res = matchObj.match(line)
            if not match_res: continue
            names, query, freq = match_res.group(1), clean_query(match_res.group(2)), int(match_res.group(3))
            #if names == "query": continue       #  ************** 交叉验证方法 **************
            if names in filter_names: continue
            if names == "query": normalQuery[query] += freq
            else: expandQuery[query] += freq
    # **************************************** jd的title ****************************************************
    print("\nread file: %s" % (config.jd_title))
    with open(config.jd_title, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(config.jd_title, 'r', encoding="utf8") as fin:
        #   百度.net开发工程师&3156
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            line = line.strip().lower()
            if TEST and i > 10: break  # TEST
            match_res = matchObj1.match(line)
            if not match_res: continue
            title, freq = clean_query(match_res.group(1)), int(match_res.group(2))
            jdTitle[title] += freq
    # **************************************** 中英文实体 ****************************************************
    print("\nread file: %s" % (config.kg_nodes_full_data))
    with open(config.kg_nodes_full_data, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(config.kg_nodes_full_data, encoding='utf8') as fin:
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            line = line.strip().lower()
            if TEST and i > 10: break  # TEST
            name = clean_query(json.loads(line)['name'].lower())
            skillEntity[name] += 10000
    normal_expand = merge_dict(normalQuery, expandQuery, 1, 5)
    normal_expand_jdtitle = merge_dict(normal_expand, jdTitle, 0, 3)
    Querys = merge_dict(normal_expand_jdtitle, skillEntity)
    return Querys

def analyse_term(query, freq):
    query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = \
        defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
    correct_sentence, senten2term, char_seg, word_seg, detail_eng = token.tokenize(query, False)
    query_freq[query] += freq
    ch_corpu_freq[' '.join(senten2term)] += freq
    for term in senten2term:
        term = term.strip()
        if not term or term in PUNCTUATION_LIST or re.fullmatch(r'([a-zA-Z]{1}|[0-9]{1})', term, re.M | re.I): continue
        word_freq[term] += freq     # 词频处理
        for t in list(term):  # 公共汉字字符处理
            if is_chinese(t):
                common_char_freq[t] += freq
        if re_en.fullmatch(term) and len(term) > 1:  # 英文集合
            english_freq[term] += freq
            en_corpu_freq[' '.join(list(term))] += freq
    return query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq

token = Tokenizer()
def gen_candidate_query_ch_corpus(Querys):
    query_corpus_ch = defaultdict(int)
    print("generate candidate query and corpus")
    sorted_Querys = sorted(Querys.items(), key=lambda d: d[1], reverse=True)
    write_file(sorted_Querys, config.candidate_query_path, "&")  # 候选的query集合
    for i, (word, freq) in enumerate(tqdm(Querys.items(), total=len(Querys))):
        query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(word, freq)
        query_corpus_ch.update(ch_corpu_freq)
    sorted_ch_corpu_freq = sorted(query_corpus_ch.items(), key=lambda d: d[1], reverse=True)
    write_file(sorted_ch_corpu_freq, config.query_corpus_ch, need_freq=False)
    write_file(sorted_ch_corpu_freq[len(sorted_ch_corpu_freq) - 1000:], config.query_corpus_ch + '_test', need_freq=False)

def gen_eng_corpus(Querys):
    english = defaultdict(int)
    print("generate english train corpus")
    for i, (word, freq) in enumerate(tqdm(Querys.items(), total=len(Querys))):
        query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(word, freq)
        english.update(en_corpu_freq)
    sorted_english = sorted(english.items(), key=lambda d: d[1], reverse=True)
    write_file(sorted_english, config.query_corpus_en, need_freq=False)
    write_file(sorted_english[len(sorted_english) - 1000:], config.query_corpus_en + '_test', need_freq=False)

def gen_common_chinese_set(Querys):
    common_chinese = defaultdict(int)
    print("generate common chinese set")
    for i, (word, freq) in enumerate(tqdm(Querys.items(), total=len(Querys))):
        query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(word, freq)
        common_chinese.update(common_char_freq)
    _common_chinese = {k: v for k, v in common_chinese.items() if v > -30}
    sorted_common_chinese = sorted(_common_chinese.items(), key=lambda d: d[1], reverse=True)
    write_file(sorted_common_chinese, config.common_char_path)  # 公共汉字符集，构造一级编辑距离的纠错候选集

def gen_english_set(Querys):
    english = defaultdict(int)
    print("generate english set")
    for i, (word, freq) in enumerate(tqdm(Querys.items(), total=len(Querys))):
        query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(word, freq)
        english.update(english_freq)
    _english = {k: v for k, v in english.items() if v > -50}
    sorted_english = sorted(_english.items(), key=lambda d: d[1], reverse=True)
    write_file(sorted_english, config.english_path)  # 英语单词集合

def gen_word_freq(Querys):
    wordfreq = defaultdict(int)
    print("generate word freqence set")
    for i, (word, freq) in enumerate(tqdm(Querys.items(), total=len(Querys))):
        query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(word, freq)
        wordfreq.update(word_freq)
    _wordfreq = {k: v for k, v in wordfreq.items() if v > -20}
    sorted_wordfreq = sorted(_wordfreq.items(), key=lambda d: d[1], reverse=True)
    write_file(sorted_wordfreq, config.word_freq_path)  # 词频

TEST = True
def gen_train_data():
    Querys = load_corpu_file()
    gen_candidate_query_ch_corpus(Querys)          # 产生候选的query和中文训练语料
    gen_eng_corpus(Querys)                          # 产生英文训练语料
    gen_common_chinese_set(Querys)                        # 产生公共汉字字符集合
    gen_english_set(Querys)                             # 英语单词集合
    gen_word_freq(Querys)                           # 产生词频集合

def write_file(obj, file_path, sep=" ", need_freq=True):
    print("\nwrite file: %s" % (file_path))
    with open(file_path, 'w', encoding="utf8") as fin:
        for k, v in tqdm(obj, total=len(obj)):
            if len(k) == 0: continue
            if need_freq: fin.write(k + sep + str(v) + "\n")
            else: fin.write(k + "\n")

def gen_normal_query():
    normalQuery = defaultdict(int)
    print("\nread file: %s" % (config.original_query_path))
    with open(config.original_query_path, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(config.original_query_path, 'r', encoding="utf8") as fin:
        #   query@百度.net开发工程师&3156
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            match_res = matchObj.match(line)
            if not match_res: continue
            names, query, freq = match_res.group(1), match_res.group(2), int(match_res.group(3))
            if names != "query": continue
            normalQuery[query] += freq
    sorted_normalQuery = sorted(normalQuery.items(), key=lambda d: d[1], reverse=True)
    write_file(sorted_normalQuery, config.query, '&')

def gen():
    queryfreq, chcorpufreq, encorpufreq, wordfreq, commoncharfreq, englishfreq = \
        defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
    # **************************************** query和cv的信息 ****************************************************
    print("\nread file: %s" % (config.original_query_path))
    with open(config.original_query_path, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(config.original_query_path, 'r', encoding="utf8") as fin:
        #   query@百度.net开发工程师&3156
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            line = line.strip().lower()
            if TEST and i > 10: break          #   TEST
            match_res = matchObj.match(line)
            if not match_res: continue
            names, query, freq = match_res.group(1), clean_query(match_res.group(2)), int(match_res.group(3))
            if names == "query": continue       #  ************** 交叉验证方法 **************
            if names in filter_names: continue
            query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(query, freq)
            queryfreq.update(query_freq); chcorpufreq.update(ch_corpu_freq); encorpufreq.update(en_corpu_freq); wordfreq.update(word_freq)
            commoncharfreq.update(common_char_freq); englishfreq.update(english_freq)
            a=1
    # **************************************** jd的title ****************************************************
    print("\nread file: %s" % (config.jd_title))
    with open(config.jd_title, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(config.jd_title, 'r', encoding="utf8") as fin:
        #   百度.net开发工程师&3156
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            line = line.strip().lower()
            if TEST and i > 10: break  # TEST
            match_res = matchObj1.match(line)
            if not match_res: continue
            title, freq = clean_query(match_res.group(1)), int(match_res.group(2))
            query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(query, freq)
            queryfreq.update(query_freq); chcorpufreq.update(ch_corpu_freq); encorpufreq.update(en_corpu_freq); wordfreq.update(word_freq)
            commoncharfreq.update(common_char_freq); englishfreq.update(english_freq)
    # **************************************** 中英文实体 ****************************************************
    print("\nread file: %s" % (config.kg_nodes_full_data))
    with open(config.kg_nodes_full_data, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(config.kg_nodes_full_data, encoding='utf8') as fin:
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            line = line.strip().lower()
            if TEST and i > 10: break  # TEST
            name = clean_query(json.loads(line)['name'].lower())
            query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(name, 10000)
            queryfreq.update(query_freq); chcorpufreq.update(ch_corpu_freq); encorpufreq.update(en_corpu_freq); wordfreq.update(word_freq)
            commoncharfreq.update(common_char_freq); englishfreq.update(english_freq)
    _queryfreq = {k: v for k, v in queryfreq.items() if v > 2}; _chcorpufreq = {k: v for k, v in chcorpufreq.items() if v > 5}
    _wordfreq = {k: v for k, v in wordfreq.items() if v > 100}; _commoncharfreq = {k: v for k, v in commoncharfreq.items() if v > 100}
    _englishfreq = {k: v for k, v in englishfreq.items() if v > 5}
    sorted_queryfreq = sorted(_queryfreq.items(), key=lambda d: d[1], reverse=True); sorted_chcorpufreq = sorted(_chcorpufreq.items(), key=lambda d: d[1], reverse=True)
    sorted_encorpufreq = sorted(encorpufreq.items(), key=lambda d: d[1], reverse=True); sorted_wordfreq = sorted(_wordfreq.items(), key=lambda d: d[1], reverse=True)
    sorted_commoncharfreq = sorted(_commoncharfreq.items(), key=lambda d: d[1], reverse=True); sorted_englishfreq = sorted(_englishfreq.items(), key=lambda d: d[1], reverse=True)
    write_file(sorted_queryfreq, config.candidate_query_path, '&'); write_file(sorted_chcorpufreq, config.query_corpus_ch, need_freq=False)
    write_file(sorted_encorpufreq, config.query_corpus_en, need_freq=False); write_file(sorted_wordfreq, config.word_freq_path)
    write_file(sorted_commoncharfreq, config.common_char_path); write_file(sorted_englishfreq, config.english_path)

def post_handle(path):
    match = re.compile(r'(.+) ([0-9]+)')
    filt = ['di', 'lv', 'ei', 'ka']
    print("\nread file: %s" % (path)); results = []; pingying = []
    with open(path, encoding='utf8') as fin:
        num_lines = len(fin.readlines())
    with open(path, encoding='utf8') as fin:
        for i, line in enumerate(tqdm(fin, total=num_lines)):
            m = match.match(line)
            word, freq = m.group(1), int(m.group(2))
            if pinyin2hanzi([word], 1) and freq < 100 and word not in filt: pingying.append(line)
            else: results.append(line)
    with open(config.pingying, 'w', encoding='utf8') as fin:
        for i, line in enumerate(tqdm(pingying, total=len(pingying))):
            fin.write(line)

if __name__ == "__main__":
    pass
    # handle_corpus()
    #gen_normal_query()
    #gen_train_data()
    #gen()
    post_handle(config.english_path)
