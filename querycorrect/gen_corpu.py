import re, json, jieba, math
from config import config
from tqdm import tqdm
from collections import defaultdict
from seg_utils import Tokenizer
from utils import PUNCTUATION_LIST, is_chinese, re_en, clean_query, BLACK_WORDS, SPECIAL_WORDS
from  sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
import numpy as np

token = Tokenizer()
def analyse_term(query, freq):
    query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = \
        defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
    correct_sentence, senten2term, char_seg, word_seg, detail_eng = token.tokenize(query, False)
    query_freq[query] += freq
    ch_corpu_freq[' '.join(senten2term)] += freq
    for term in senten2term:
        term = term.strip().lower()
        if not term or term in PUNCTUATION_LIST or re.fullmatch(r'([a-zA-Z]{1}|[0-9]{1})', term, re.M | re.I): continue
        word_freq[term] += freq     # 词频处理
        for t in list(term):  # 公共汉字字符处理
            if is_chinese(t):
                common_char_freq[t] += freq
        if term in SPECIAL_WORDS or (re_en.fullmatch(term) and len(term) > 1):  # 英文集合
            english_freq[term] += freq
            en_corpu_freq[' '.join(list(term))] += freq
    return query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq

def write_file(obj, file_path, sep=" ", need_freq=True):
    print("\nwrite file: %s" % (file_path))
    with open(file_path, 'w', encoding="utf8") as fin:
        for k, v in tqdm(obj, total=len(obj)):
            if len(k) == 0: continue
            if need_freq: fin.write(k + sep + str(v) + "\n")
            else: fin.write(k + "\n")

def gen_train_corpus():
    ch_res, en_res, wf_res, ccf_res, ef_res = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)

    def read_file(path, th=0, mat=re.compile(r'(.+)&([0-9]+)', re.M | re.I), kg=False):
        ch_corpu, en_corpu, wf_dict, en_dict = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
        print("\nread file: %s" % (path))
        BLACK_WORDS_SET = set(BLACK_WORDS)
        num_lines = len(open(path, encoding='utf8').readlines())
        for i, line in enumerate(tqdm(open(path, encoding='utf8'), total=num_lines)):
            line = line.strip().lower()   ; line = "康师傅   it	 6"
        #    if i > 100: break
            if kg:
                name = clean_query(json.loads(line)['name'].lower())
                query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(name, 10000)
                ch_corpu.update(ch_corpu_freq), en_corpu.update(en_corpu_freq)
            else:
                match_res = mat.match(line)
                if not match_res: continue
                query, freq = clean_query(match_res.group(1)), int(match_res.group(2))
                if freq < th: continue
                #query = re.sub(u"[ ]{1,}", "", query)
                query_freq, ch_corpu_freq, en_corpu_freq, word_freq, common_char_freq, english_freq = analyse_term(query, freq)
            if set(word_freq.keys()).intersection(BLACK_WORDS_SET): continue
            ch_corpu.update({k: v for k, v in ch_corpu_freq.items() if len(k.split()) > 1}); en_corpu.update(en_corpu_freq); ccf_res.update(common_char_freq)
            wf_dict.update({k: v for k, v in word_freq.items() if len(k) > -1}); en_dict.update(english_freq)
        #print("chinese corpu length: %d" % (len(ch_corpu_res)))
        ch_res.update(ch_corpu); en_res.update(en_corpu)
        return wf_dict, en_dict

    sorch_re = re.compile(r'(.+)\t ([0-9]+)', re.M | re.I)
    #ch_d, en_d = read_file(config.query, th=-2); wf_res.update(mad_filter(ch_d)); ef_res.update(mad_filter(en_d))
    #ch_d, en_d = read_file(config.jd_title, th=100); wf_res.update(mad_filter(ch_d)); ef_res.update(mad_filter(en_d))
    ch_d, en_d = read_file(config.search_data, th=-10, mat=sorch_re); wf_res.update(mad_filter(ch_d, 10.99)); ef_res.update(mad_filter(en_d, 10.9)) # 0.9999,0.98
    #ch_d, en_d = read_file(config.kg_nodes_full_data, kg=True); wf_res.update(mad_filter(ch_d)); ef_res.update(mad_filter(en_d))
    sorted_ch_corpu = sorted(ch_res.items(), key=lambda d: d[1], reverse=True)
    sorted_en_corpu = sorted(en_res.items(), key=lambda d: d[1], reverse=True)
    sorted_wordfreq = sorted(wf_res.items(), key=lambda d: d[1], reverse=True)
    sorted_commoncharfreq = sorted(ccf_res.items(), key=lambda d: d[1], reverse=True)
    sorted_englishfreq = sorted(ef_res.items(), key=lambda d: d[1], reverse=True)
    #write_file(sorted_ch_corpu, config.query_corpus_ch, need_freq=False); write_file(sorted_en_corpu, config.query_corpus_en, need_freq=False); #exit()

    write_file(sorted_wordfreq, config.word_freq_path)
    write_file(sorted_commoncharfreq, config.common_char_path)
    write_file(sorted_englishfreq, config.english_path)
    a=1

def mad_filter(word_freq_dict, ratio=0.9):
    word_freq_tfidf = [[word, freq, freq * float(word_tf_idf_tfidf.get(word, [0,0,0])[1])] for word, freq in word_freq_dict.items()]
    scores = np.array([e[2] for e in word_freq_tfidf])
    median = np.median(scores)
    sorted_word_freq_tfidf = sorted(word_freq_tfidf, key=lambda d: d[2], reverse=True)
    #res = [[e[0], e[1]] for i, e in enumerate(word_freq_tfidf) if e[0] not in BLACK_WORDS and e[2] > median * 0.1]
    #res = [[e[0], e[1]] for i, e in enumerate(sorted_word_freq_tfidf[: math.ceil(len(sorted_word_freq_tfidf) * ratio)]) if e[0] not in BLACK_WORDS]
    res = {e[0]: e[1] for i, e in enumerate(sorted_word_freq_tfidf[: math.ceil(len(sorted_word_freq_tfidf) * ratio)]) if e[0] not in BLACK_WORDS}
    return res

def word_score():
    path = config.jd_title
    m = re.compile(u'(.+)&([0-9]+)', re.M | re.I)
    tf, word_cnt, total_num, res = defaultdict(int), defaultdict(int), 0, []
    num_lines = len(open(path, encoding='utf8').readlines())
    for i, line in enumerate(tqdm(open(path, encoding='utf8'), total=num_lines)):
        mr = m.match(line)
        if not mr: continue
        total_num += 1
        seg_line = mr.group(1).split()
        for word in seg_line:
            word_cnt[word] += 1
            if int(mr.group(2)) > 2:
                tf[word] += int(mr.group(2))
    for word, cnt in word_cnt.items():       # [word, tf, idf, tf*idf]
        tfvalue, idfvalue = tf.get(word, 0), round(math.log10(total_num / (cnt + 1)), 3)
        res.append([word, tfvalue, idfvalue, round(tfvalue * idfvalue, 3)])
    sorted_res = sorted(res, key=lambda d: d[3], reverse=True)
    with open("data/word_idf", "w", encoding="utf8") as fin:
        for ele in sorted_res:
            fin .write(' '.join([str(e) for e in ele]) + "\n")
    a=1

word_tf_idf_tfidf = {line.strip().split()[0]: line.strip().split()[1:] for line in open("data/word_idf", encoding="utf8").readlines() \
                     if float(line.strip().split()[-1]) > -50}

if __name__ == "__main__":
    #analyse_term("新东方学校", 1)
    #word_score()
    gen_train_corpus()