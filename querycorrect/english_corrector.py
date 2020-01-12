import re, Levenshtein, os, logging, codecs, math
from collections import Counter, defaultdict
from config import config, LanguageModel
from utils import BLACK_WORDS

def _get_custom_confusion_dict(path):     # 取自定义困惑集。dict, {variant: origin}, eg: {"交通先行": "交通限行"}
    confusion = {}
    with codecs.open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#'):
                continue
            info = line.split()
            if len(info) < 2:
                continue
            variant = info[0]
            origin = info[1]
            confusion[variant] = origin
    return confusion

def load_word_freq_dict(path, freq_th=0, default_freq=1):      # 加载切词词典
    word_freq = defaultdict(int)
    if not os.path.exists(path):
        logging.warning("file not exists:" + path)
        return word_freq
    with codecs.open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#'): continue
            info = line.split()
            freq = int(info[1]) if len(info) > 1 else default_freq         # 取词频，默认 default_freq
            if freq < freq_th: continue
            word_freq[info[0]] += freq
    return word_freq

def edits1(word):
    "All edits that are one edit away from `word`."
    letters    = 'abcdefghijklmnopqrstuvwxyz'
    splits     = [(word[:i], word[i:])    for i in range(len(word) + 1)]
    deletes    = [L + R[1:]               for L, R in splits if R]
    transposes = [L + R[1] + R[0] + R[2:] for L, R in splits if len(R)>1]
    replaces   = [L + c + R[1:]           for L, R in splits if R for c in letters]
    inserts    = [L + c + R               for L, R in splits for c in letters]
    return set(deletes + transposes + replaces + inserts)

def edits2(word):
    "All edits that are two edits away from `word`."
    return (e2 for e1 in edits1(word) for e2 in edits1(e1))

def selecttopk(items, k, reverse=True):
    res = []; scores = set()
    sorted_items = sorted(items, key=lambda d: d[1], reverse=reverse)
    for w, s in sorted_items:
        scores.add(s)
        if len(scores) > k:
            break
        res.append((w, s))
    return res

class EnglishCorrector(LanguageModel):
    def __init__(self):
        super(EnglishCorrector, self).__init__()
        self.WORDS = load_word_freq_dict(config.english_path, config.english_th)
        self.WORDS.update(load_word_freq_dict(config.custom_word_freq_path))
        self.custom_confusion_dict = _get_custom_confusion_dict(config.custom_confusion_path)

    def eng_ppl_score(self, words):         # 取语言模型困惑度得分，越小句子越通顺。words: list, 以词或字切分
        return self.eng_lm.perplexity(' '.join(words))

    def sort_candidates(self, word, candidates, edit_sort=True, topk=3):
        if edit_sort:
            candi_scores = [(e, round(Levenshtein.ratio(e, word), 3)) for e in candidates]
            sorted_candis = selecttopk(candi_scores, topk)
        else:
            candi_scores = [(e, self.eng_ppl_score(list(e))) for e in candidates]
            sorted_candis = selecttopk(candi_scores, topk, False)
        candidate = [e[0] for e in sorted_candis]
        return candidate, sorted_candis

    def correction(self, word):
        "Most probable spelling correction for word."
        word = word.lower()
        if word in self.custom_confusion_dict:
            return self.custom_confusion_dict.get(word)
        if word not in BLACK_WORDS and (len(word) == 1 or self.WORDS.get(word, 0) > 0 or not self.WORDS):
            return word
        candis = self.candidates(word, 0.5)
        if not candis:
            return word
        candi, sorted_candi = self.sort_candidates(word, candis, True)   # 编辑距离排序
        candidate, sorted_scores = self.sort_candidates(word, candi, False)  # 语言模型排序
        #scores = [(e, self.eng_ppl_score(list(e))) for e in candis]
        #sorted_scores = sorted(scores, key=lambda d: d[1])
        return candidate[0]

    def candidates(self, word, frac=1):
        "Generate possible spelling corrections for word."
        res = set([word])
        candidate_word = self.known_word([word])
        candidate_edit1 = self.known_word(edits1(word))
        candidate_edit2 = self.known_word(edits2(word))
        res.update(candidate_word)
        res.update(candidate_edit1)
        res.update(candidate_edit2)
        word_freq = {e: self.WORDS.get(e, 0) for e in res if e not in BLACK_WORDS}
        sorted_word_freq = sorted(word_freq.items(), key=lambda d: d[1], reverse=True)
        cut_index = math.ceil(len(sorted_word_freq) * frac)
        while cut_index + 1 < len(sorted_word_freq) - 1 and sorted_word_freq[cut_index + 1][1] == sorted_word_freq[cut_index][1]: cut_index += 1
        sorted_word_freq_top = sorted_word_freq[: cut_index]
        result = set(w for w, f in sorted_word_freq_top)
        return result

    def known_word(self, words):
        "The subset of `words` that appear in the dictionary of WORDS."
        res = set(w for w in words if w in self.WORDS)
        return res

if __name__ == '__main__':
    d = Levenshtein.distance("andriord", "android")
    ec = EnglishCorrector()
    for e in ['jav','slot','andrid', 'andrd','exel','exact','misfit','arcsoft','andriod','androi','Andriord']:
        e='C/S'
        print(e + ' -> ' + ec.correction(e))
