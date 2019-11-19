import re, os, codecs, logging, jieba, copy, json, traceback
from english_corrector import EnglishCorrector
from config import config
from utils import re_en, is_alphabet_string, PUNCTUATION_LIST, is_chinese

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

def load_word_freq_dict(path):      # 加载切词词典
    word_freq = {}
    if not os.path.exists(path):
        logging.warning("file not exists:" + path)
        return word_freq
    with codecs.open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#'):
                continue
            info = line.split()
            if len(info) < 1:
                continue
            word = info[0]
            # 取词频，默认1
            freq = int(info[1]) if len(info) > 1 else 1
            word_freq[word] = freq
    return word_freq

class Tokenizer(EnglishCorrector):
    def __init__(self):
        super(Tokenizer, self).__init__()
        self.model = jieba
        self.model.default_logger.setLevel(logging.ERROR)
        # 初始化大词典
        if os.path.exists(config.word_freq_path):
            self.model.set_dictionary(config.word_freq_path)
        # 加载用户自定义词典
        custom_word_freq = load_word_freq_dict(config.custom_word_freq_path)
        self.custom_filter_word = copy.deepcopy(custom_word_freq)
        person_names = load_word_freq_dict(config.person_name_path)
        place_names = load_word_freq_dict(config.place_name_path)
        stopwords = load_word_freq_dict(config.stopwords_path)
        custom_word_freq.update(person_names)
        custom_word_freq.update(place_names)
        custom_word_freq.update(stopwords)
        if custom_word_freq:
            for w, f in custom_word_freq.items():
                self.model.add_word(w, freq=f)
        # 加载混淆集词典
        custom_confusion = _get_custom_confusion_dict(config.custom_confusion_path)
        if custom_confusion:
            for k, word in custom_confusion.items():
                # 添加到分词器的自定义词典中
                self.model.add_word(k)
                self.model.add_word(word)

    def tokenize(self, sentence, correct_eng=True):
        correct_sentence, senten2term, char_seg, word_seg, detail_eng, char_index, word_index = '', [], [], [], [], 0, 0
        #a=re_en.split(sentence)#; sentence = "上海百度公司java,elastic开法工程师"; aa=list(self.model.tokenize(sentence))
        for word in re_en.split(sentence):
            word = word.strip()
            if word in ['', ' ']: continue
            if re_en.fullmatch(word):   # 英文处理
                if word in self.custom_filter_word: rword = word
                elif correct_eng: rword = self.correction(word)
                else: rword = word
                word_seg.append((rword, word_index, word_index+len(rword)))
                senten2term.append(rword)
                char_seg.append((rword, char_index, char_index+len(rword)))
                if rword != word:       # 记录英文纠错细节
                    detail_eng.append([word, rword, char_index, char_index+len(rword), ErrorType.english])
                char_index += len(rword)
            else:                       # 非英文处理
                model_seg = list(self.model.tokenize(word))
                word_seg.extend([(e[0], e[1]+word_index, e[2]+word_index) for e in model_seg])
                if config.char_term: senten2term.extend(list(word))
                else: senten2term.extend([e[0] for e in model_seg])
                for w in list(word):
                    char_seg.append((w, char_index, char_index+1))
                    char_index += 1
            word_index = word_seg[-1][2]
        for i in range(len(word_seg)):
            if i < len(word_seg) - 1 and is_alphabet_string(word_seg[i][0]) and  is_alphabet_string(word_seg[i+1][0]):
                correct_sentence += word_seg[i][0] + ' '
            else:
                correct_sentence += word_seg[i][0]
        return correct_sentence, senten2term, char_seg, word_seg, detail_eng

#t=Tokenizer(); a=t.tokenize("百度jaca开法工程师、c++后台", False)
class ErrorType(object):
    confusion, word, term, english = 'confusion', 'word', 'term', 'english'

def load_char_set(path):
    words = set()
    if not os.path.exists(path):
        logging.warning("file not exists:" + path)
        return words
    with codecs.open(path, 'r', encoding='utf-8') as f:
        for w in f:
            words.add(w.strip())
    return words

def load_same_pinyin(path, sep='\t'):       # 加载同音字
    result = dict()
    if not os.path.exists(path):
        logging.warning("file not exists:" + path)
        return result
    with codecs.open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#'):
                continue
            parts = line.split(sep)
            if parts:
                key_char, same_pron_same_tone, same_pron_diff_tone = parts[0], set(), set()
                if len(parts) > 1: same_pron_same_tone = set(list(parts[1]))
                if len(parts) > 2: same_pron_diff_tone = set(list(parts[2]))
                value = same_pron_same_tone.union(same_pron_diff_tone)
                if len(key_char) > 1 or not value:
                    continue
                result[key_char] = value
    return result

def load_same_stroke(path, sep='\t'):   # 加载形似字
    result = dict()
    if not os.path.exists(path):
        logging.warning("file not exists:" + path)
        return result
    with codecs.open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('#'):
                continue
            parts = line.split(sep)
            if parts and len(parts) > 1:
                for i, c in enumerate(parts):
                    result[c] = set(list(parts[:i] + parts[i + 1:]))
    return result

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

if __name__ == "__main__":
    a = list(jieba.tokenize("吴燕青,3年工作经验"))
