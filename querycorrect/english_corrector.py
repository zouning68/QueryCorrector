import re, Levenshtein, os, logging, codecs
from collections import Counter, defaultdict
from config import config, LanguageModel

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

class EnglishCorrector(LanguageModel):
    def __init__(self):
        super(EnglishCorrector, self).__init__()
        self.WORDS = load_word_freq_dict(config.english_path)
        self.custom_confusion_dict = _get_custom_confusion_dict(config.custom_confusion_path)
        a=1

    def eng_ppl_score(self, words):         # 取语言模型困惑度得分，越小句子越通顺。words: list, 以词或字切分
        return self.eng_lm.perplexity(' '.join(words))

    def sort_candidates(self, word, candidates, edit_sort=True, topk=5):
        if edit_sort:
            candi_scores = [(e, round(Levenshtein.ratio(e, word), 3)) for e in candidates]
            sorted_candis = sorted(candi_scores, key=lambda d: d[1], reverse=True)[:topk]
        else:
            candi_scores = [(e, self.eng_ppl_score(list(e))) for e in candidates]
            sorted_candis = sorted(candi_scores, key=lambda d: d[1])[:topk]
        candidate = [e[0] for e in sorted_candis]
        return candidate, sorted_candis

    def correction(self, word):
        "Most probable spelling correction for word."
        if word in self.custom_confusion_dict:
            return self.custom_confusion_dict.get(word)
        if len(word) == 1 or word in self.WORDS or not self.WORDS:
            return word
        candis = self.candidates(word, 0.8)
        if not candis:
            return word
        candi_edit, sorted_candi_edit = self.sort_candidates(word, candis)   # 编辑距离排序
        candi_lm, sorted_scores = self.sort_candidates(word, candi_edit, False)  # 语言模型排序
        #scores = [(e, self.eng_ppl_score(list(e))) for e in candis]
        #sorted_scores = sorted(scores, key=lambda d: d[1])
        return sorted_scores[0][0]

    def candidates(self, word, frac=1):
        "Generate possible spelling corrections for word."
        res = set([word])
        candidate_word = self.known_word([word])
        candidate_edit1 = self.known_word(edits1(word))
        candidate_edit2 = self.known_word(edits2(word))
        res.update(candidate_word)
        res.update(candidate_edit1)
        res.update(candidate_edit2)
        word_freq = {e: self.WORDS.get(e, 0) for e in res}
        sorted_word_freq = sorted(word_freq.items(), key=lambda d: d[1], reverse=True)
        sorted_word_freq_top = sorted_word_freq[: int(len(sorted_word_freq) * frac)]
        res = set(w for w, f in sorted_word_freq_top)
        return res

    def known_word(self, words):
        "The subset of `words` that appear in the dictionary of WORDS."
        res = set(w for w in words if w in self.WORDS)
        return res

if __name__ == '__main__':
    d = Levenshtein.distance("andio", "android")
    ec = EnglishCorrector()
    print(ec.correction("wold"))
