import codecs, operator, os, time, logging, kenlm, jieba, re
from pypinyin import lazy_pinyin
import numpy as np

#***********************************************************************************************************************
PUNCTUATION_LIST = ".。,，,、?？:：;；{}[]【】“‘’”《》/!！%……（）<>@#$~^￥%&*\"\'=+-_——「」"
re_ch = re.compile(u"([\u4e00-\u9fa5])",re.S)
re_en = re.compile(u"([a-zA-Z]*)",re.S)
def is_alphabet_string(string):     # 判断是否全部为英文字母
    for c in string:
        if c < 'a' or c > 'z':
            return False
    return True

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

def edit_distance_word(word, char_set):     # all edits that are one edit away from 'word'
    splits = [(word[:i], word[i:]) for i in range(len(word) + 1)]
    transposes = [L + R[1] + R[0] + R[2:] for L, R in splits if len(R) > 1]
    replaces = [L + c + R[1:] for L, R in splits if R for c in char_set]
    return set(transposes + replaces)

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

class Config:
    def __init__(self):
        pwd_path = os.path.abspath(os.path.dirname(__file__))
        self.word_freq_path = os.path.join(pwd_path, 'data/word_freq.txt')  # 通用分词词典文件  format: 词语 词频
        self.common_char_path = os.path.join(pwd_path, 'data/common_char_set.txt')   # 中文常用字符集
        self.same_pinyin_path = os.path.join(pwd_path, 'data/same_pinyin.txt')   # 同音字
        self.same_stroke_path = os.path.join(pwd_path, 'data/same_stroke.txt')   # 形似字
        self.language_model_path = os.path.join(pwd_path, 'data/kenlm/people_chars_lm.klm')  # language model path
        self.custom_confusion_path = os.path.join(pwd_path, 'data/custom_confusion.txt') # 用户自定义错别字混淆集 format:变体 本体 本体词词频（可省略）
        self.custom_word_freq_path = os.path.join(pwd_path, 'data/custom_word_freq.txt') # 用户自定义分词词典  format: 词语 词频
        self.person_name_path = os.path.join(pwd_path, 'data/person_name.txt')   # 知名人名词典 format: 词语 词频
        self.place_name_path = os.path.join(pwd_path, 'data/place_name.txt') # 地名词典 format: 词语 词频
        self.stopwords_path = os.path.join(pwd_path, 'data/stopwords.txt')   # 停用词
config = Config()

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
        seg_res, cur_index = [], 0    ;a=re_en.split(sentence)#; sentence = "上海百度公司java,elastic开法工程师"; aa=list(self.model.tokenize(sentence))
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

class ErrorType(object):    # error_type = {"confusion": 1, "word": 2, "char": 3}
    confusion, word, char = 'confusion', 'word', 'char'

class Detector(object):
    def __init__(self, language_model_path=config.language_model_path,
                 word_freq_path=config.word_freq_path,
                 custom_word_freq_path=config.custom_word_freq_path,
                 custom_confusion_path=config.custom_confusion_path,
                 person_name_path=config.person_name_path,
                 place_name_path=config.place_name_path,
                 stopwords_path=config.stopwords_path):
        self.name = 'detector'
        self.language_model_path = language_model_path
        self.word_freq_path = word_freq_path
        self.custom_word_freq_path = custom_word_freq_path
        self.custom_confusion_path = custom_confusion_path
        self.person_name_path = person_name_path
        self.place_name_path = place_name_path
        self.stopwords_path = stopwords_path
        self.is_char_error_detect = True
        self.is_word_error_detect = True
        self.initialized_detector = False
        self.enable_rnnlm = False
        #initialize detector dict sets
        t1 = time.time()
        self.lm = kenlm.Model(self.language_model_path)
        logging.debug('Loaded language model: %s, spend: %s s' % (self.language_model_path, str(time.time() - t1)))
        # 词、频数 dict
        t2 = time.time()
        self.word_freq = self.load_word_freq_dict(self.word_freq_path)
        t3 = time.time()
        logging.debug('Loaded word freq file: %s, size: %d, spend: %s s' % (self.word_freq_path, len(self.word_freq), str(t3 - t2)))
        # 自定义混淆集
        self.custom_confusion = self._get_custom_confusion_dict(self.custom_confusion_path)
        t4 = time.time()
        logging.debug('Loaded confusion file: %s, size: %d, spend: %s s' % (self.custom_confusion_path, len(self.custom_confusion), str(t4 - t3)))
        # 自定义切词词典
        self.custom_word_freq = self.load_word_freq_dict(self.custom_word_freq_path)
        self.person_names = self.load_word_freq_dict(self.person_name_path)
        self.place_names = self.load_word_freq_dict(self.place_name_path)
        self.stopwords = self.load_word_freq_dict(self.stopwords_path)
        # 合并切词词典及自定义词典
        self.custom_word_freq.update(self.person_names)
        self.custom_word_freq.update(self.place_names)
        self.custom_word_freq.update(self.stopwords)

        self.word_freq.update(self.custom_word_freq)
        t5 = time.time()
        logging.debug('Loaded custom word file: %s, size: %d, spend: %s s' % (self.custom_confusion_path, len(self.custom_word_freq), str(t5 - t4)))
        self.tokenizer = Tokenizer(dict_path=self.word_freq_path, custom_word_freq_dict=self.custom_word_freq, custom_confusion_dict=self.custom_confusion)
        self.initialized_detector = True

    @staticmethod
    def load_word_freq_dict(path):      # 加载切词词典
        word_freq = {}
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

    def _get_custom_confusion_dict(self, path):     # 取自定义困惑集。dict, {variant: origin}, eg: {"交通先行": "交通限行"}
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
                freq = int(info[2]) if len(info) > 2 else 1
                self.word_freq[origin] = freq
                confusion[variant] = origin
        return confusion

    def set_language_model_path(self, path):
        self.lm = kenlm.Model(path)
        logging.debug('Loaded language model: %s' % path)

    def set_custom_confusion_dict(self, path):
        custom_confusion = self._get_custom_confusion_dict(path)
        self.custom_confusion.update(custom_confusion)
        logging.debug('Loaded confusion path: %s, size: %d' % (path, len(custom_confusion)))

    def set_custom_word(self, path):
        word_freqs = self.load_word_freq_dict(path)
        # 合并字典
        self.custom_word_freq.update(word_freqs)
        # 合并切词词典及自定义词典
        self.word_freq.update(self.custom_word_freq)
        self.tokenizer = Tokenizer(dict_path=self.word_freq_path, custom_word_freq_dict=self.custom_word_freq, custom_confusion_dict=self.custom_confusion)
        for k, v in word_freqs.items():
            self.set_word_frequency(k, v)
        logging.debug('Loaded custom word path: %s, size: %d' % (path, len(word_freqs)))

    def enable_char_error(self, enable=True):       # is open char error detect
        self.is_char_error_detect = enable

    def enable_word_error(self, enable=True):       # is open word error detect
        self.is_word_error_detect = enable

    def ngram_score(self, chars):       # 取n元文法得分。chars: list, 以词或字切分
        a=' '.join(chars)
        return self.lm.score(' '.join(chars), bos=False, eos=False)

    def ppl_score(self, words):         # 取语言模型困惑度得分，越小句子越通顺。words: list, 以词或字切分
        return self.lm.perplexity(' '.join(words))

    def word_frequency(self, word):     # 取词在样本中的词频
        return self.word_freq.get(word, 0)

    def set_word_frequency(self, word, num):        # 更新在样本中的词频
        self.word_freq[word] = num
        return self.word_freq

    @staticmethod
    def _check_contain_error(maybe_err, maybe_errors):
        #检测错误集合(maybe_errors)是否已经包含该错误位置（maybe_err)。maybe_err: [error_word, begin_pos, end_pos, error_type]
        error_word_idx = 0
        begin_idx = 1
        end_idx = 2
        for err in maybe_errors:
            if maybe_err[error_word_idx] in err[error_word_idx] and maybe_err[begin_idx] >= err[begin_idx] and \
                            maybe_err[end_idx] <= err[end_idx]:
                return True
        return False

    def _add_maybe_error_item(self, maybe_err, maybe_errors):       # 新增错误
        if maybe_err not in maybe_errors and not self._check_contain_error(maybe_err, maybe_errors):
            maybe_errors.append(maybe_err)

    @staticmethod
    def _get_maybe_error_index(scores, ratio=0.6745, threshold=1.4):
        """
        取疑似错字的位置，通过平均绝对离差（MAD）
        :param scores: np.array
        :param threshold: 阈值越小，得到疑似错别字越多
        :return: 全部疑似错误字的index: list
        """
        result = []
        scores = np.array(scores)
        if len(scores.shape) == 1:
            scores = scores[:, None]
        median = np.median(scores, axis=0)  # get median of all scores
        margin_median = np.abs(scores - median).flatten()  # deviation from the median
        # 平均绝对离差值
        med_abs_deviation = np.median(margin_median)
        if med_abs_deviation == 0:
            return result
        y_score = ratio * margin_median / med_abs_deviation
        # 打平
        scores = scores.flatten()
        maybe_error_indices = np.where((y_score > threshold) & (scores < median))
        # 取全部疑似错误字的index
        result = list(maybe_error_indices[0])
        return result

    @staticmethod
    def _get_maybe_error_index_by_rnnlm(scores, n=3):
        """
        取疑似错字的位置，通过平均值上下三倍标准差之间属于正常点
        :param scores: list, float
        :param threshold: 阈值越小，得到疑似错别字越多
        :return: 全部疑似错误字的index: list
        """
        std = np.std(scores, ddof=1)
        mean = np.mean(scores)
        down_limit = mean - n * std
        upper_limit = mean + n * std
        maybe_error_indices = np.where((scores > upper_limit) | (scores < down_limit))
        # 取全部疑似错误字的index
        result = list(maybe_error_indices[0])
        return result

    @staticmethod
    def is_filter_token(token):
        result = False
        # pass blank
        if not token.strip():
            result = True
        # pass punctuation
        if token in PUNCTUATION_LIST:
            result = True
        # pass num
        if token.isdigit():
            result = True
        # pass alpha
        #if is_alphabet_string(token.lower()):
        #    result = True
        return result

    def detect(self, sentence):
        """
        检测句子中的疑似错误信息，包括[词、位置、错误类型]
        :param sentence:
        :return: list[list], [error_word, begin_pos, end_pos, error_type]
        """
        maybe_errors = []
        if not sentence.strip():
            return maybe_errors
        # 文本归一化
        sentence = uniform(sentence)
        # 切词
        tokens = self.tokenizer.tokenize(sentence)
        # print(tokens)
        # 自定义混淆集加入疑似错误词典
        for confuse in self.custom_confusion:
            idx = sentence.find(confuse)
            if idx > -1:
                maybe_err = [confuse, idx, idx + len(confuse), ErrorType.confusion]
                self._add_maybe_error_item(maybe_err, maybe_errors)

        if self.is_word_error_detect:
            # 未登录词加入疑似错误词典
            for word, begin_idx, end_idx in tokens:
                # pass filter word
                if self.is_filter_token(word):
                    continue
                # pass in dict
                if word in self.word_freq:
                    continue
                maybe_err = [word, begin_idx, end_idx, ErrorType.word]
                self._add_maybe_error_item(maybe_err, maybe_errors)

        if self.is_char_error_detect:
            # 语言模型检测疑似错误字
            if self.enable_rnnlm:
                scores = self.char_scores(sentence)
                # 取疑似错字信息
                for i in self._get_maybe_error_index_by_rnnlm(scores):
                    token = sentence[i]
                    # pass filter word
                    if self.is_filter_token(token):
                        continue
                    maybe_err = [token, i, i + 1, ErrorType.char]  # token, begin_idx, end_idx, error_type
                    self._add_maybe_error_item(maybe_err, maybe_errors)
            else:
                try:
                    ngram_avg_scores = []
                    for n in [2, 3]:
                        scores = []
                        for i in range(len(sentence) - n + 1):
                            word = sentence[i:i + n]
                            score = self.ngram_score(list(word))
                            scores.append(score)
                        if not scores:
                            continue
                        # 移动窗口补全得分
                        for _ in range(n - 1):
                            scores.insert(0, scores[0])
                            scores.append(scores[-1])
                        avg_scores = [sum(scores[i:i + n]) / len(scores[i:i + n]) for i in range(len(sentence))]
                        ngram_avg_scores.append(avg_scores)

                    # 取拼接后的n-gram平均得分
                    sent_scores = list(np.average(np.array(ngram_avg_scores), axis=0))
                    # 取疑似错字信息
                    for i in self._get_maybe_error_index(sent_scores):
                        token = sentence[i]
                        # pass filter word
                        if self.is_filter_token(token):
                            continue
                        maybe_err = [token, i, i + 1, ErrorType.char]  # token, begin_idx, end_idx, error_type
                        self._add_maybe_error_item(maybe_err, maybe_errors)
                except IndexError as ie:
                    logging.warning("index error, sentence:" + sentence + str(ie))
                except Exception as e:
                    logging.warning("detect error, sentence:" + sentence + str(e))
        return sorted(maybe_errors, key=lambda k: k[1], reverse=False)

#***********************************************************************************************************************
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
            if parts and len(parts) > 2:
                key_char = parts[0]
                same_pron_same_tone = set(list(parts[1]))
                same_pron_diff_tone = set(list(parts[2]))
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

class Corrector(Detector):
    def __init__(self, common_char_path=config.common_char_path,
                 same_pinyin_path=config.same_pinyin_path,
                 same_stroke_path=config.same_stroke_path,
                 language_model_path=config.language_model_path,
                 word_freq_path=config.word_freq_path,
                 custom_word_freq_path=config.custom_word_freq_path,
                 custom_confusion_path=config.custom_confusion_path,
                 person_name_path=config.person_name_path,
                 place_name_path=config.place_name_path,
                 stopwords_path=config.stopwords_path):
        super(Corrector, self).__init__(language_model_path=language_model_path,
                                        word_freq_path=word_freq_path,
                                        custom_word_freq_path=custom_word_freq_path,
                                        custom_confusion_path=custom_confusion_path,
                                        person_name_path=person_name_path,
                                        place_name_path=place_name_path,
                                        stopwords_path=stopwords_path)
        self.name = 'corrector'
        self.common_char_path = common_char_path
        self.same_pinyin_text_path = same_pinyin_path
        self.same_stroke_text_path = same_stroke_path
        self.initialized_corrector = False
        # initialize corrector dict sets
        t1 = time.time()
        self.cn_char_set = load_char_set(self.common_char_path)  # chinese common char dict
        self.same_pinyin = load_same_pinyin(self.same_pinyin_text_path)  # same pinyin
        self.same_stroke = load_same_stroke(self.same_stroke_text_path)  # same stroke
        logging.debug("Loaded same pinyin file: %s, same stroke file: %s, spend: %.3f s." % (
            self.same_pinyin_text_path, self.same_stroke_text_path, time.time() - t1))
        self.initialized_corrector = True

    def get_same_pinyin(self, char):        # 取同音字
        return self.same_pinyin.get(char, set())

    def get_same_stroke(self, char):        # 取形似字
        return self.same_stroke.get(char, set())

    def known(self, words):     # 取得词序列中属于常用词部分
        return set(word for word in words if word in self.word_freq)

    def _confusion_char_set(self, c):
        return self.get_same_pinyin(c).union(self.get_same_stroke(c))

    def _confusion_word_set(self, word):
        confusion_word_set = set()
        candidate_words = list(self.known(edit_distance_word(word, self.cn_char_set)))
        for candidate_word in candidate_words:
            if lazy_pinyin(candidate_word) == lazy_pinyin(word):
                # same pinyin
                confusion_word_set.add(candidate_word)
        return confusion_word_set

    def _confusion_custom_set(self, word):
        confusion_word_set = set()
        if word in self.custom_confusion:
            confusion_word_set = {self.custom_confusion[word]}
        return confusion_word_set

    def generate_items(self, word, fraction=1):         # 生成纠错候选集
        candidates_1_order = []
        candidates_2_order = []
        candidates_3_order = []
        # same pinyin word
        candidates_1_order.extend(self._confusion_word_set(word))
        # custom confusion word
        candidates_1_order.extend(self._confusion_custom_set(word))
        # same pinyin char
        if len(word) == 1:
            # same one char pinyin
            confusion = [i for i in self._confusion_char_set(word[0]) if i]
            candidates_1_order.extend(confusion)
        if len(word) == 2:
            # same first char pinyin
            confusion = [i + word[1:] for i in self._confusion_char_set(word[0]) if i]
            candidates_2_order.extend(confusion)
            # same last char pinyin
            confusion = [word[:-1] + i for i in self._confusion_char_set(word[-1]) if i]
            candidates_2_order.extend(confusion)
        if len(word) > 2:
            # same mid char pinyin
            confusion = [word[0] + i + word[2:] for i in self._confusion_char_set(word[1])]
            candidates_3_order.extend(confusion)

            # same first word pinyin
            confusion_word = [i + word[-1] for i in self._confusion_word_set(word[:-1])]
            candidates_3_order.extend(confusion_word)

            # same last word pinyin
            confusion_word = [word[0] + i for i in self._confusion_word_set(word[1:])]
            candidates_3_order.extend(confusion_word)

        # add all confusion word list
        confusion_word_set = set(candidates_1_order + candidates_2_order + candidates_3_order)
        confusion_word_list = [item for item in confusion_word_set if is_chinese_string(item)]
        confusion_sorted = sorted(confusion_word_list, key=lambda k: self.word_frequency(k), reverse=True)
        return confusion_sorted[:len(confusion_word_list) // fraction + 1]

    def lm_correct_item(self, item, maybe_right_items, before_sent, after_sent):        # 通过语音模型纠正字词错误
        if item not in maybe_right_items:
            maybe_right_items.append(item)
        corrected_item = min(maybe_right_items, key=lambda k: self.ppl_score(list(before_sent + k + after_sent)))
        return corrected_item

    def correct(self, sentence):
        """
        句子改错
        :param sentence: 句子文本
        :return: 改正后的句子, list(wrong, right, begin_idx, end_idx)
        """
        detail = []
        # 长句切分为短句
        # sentences = re.split(r"；|，|。|\?\s|;\s|,\s", sentence)
        maybe_errors = self.detect(sentence)
        # trick: 类似翻译模型，倒序处理
        maybe_errors = sorted(maybe_errors, key=operator.itemgetter(2), reverse=True)
        for item, begin_idx, end_idx, err_type in maybe_errors:
            # 纠错，逐个处理
            before_sent = sentence[:begin_idx]
            after_sent = sentence[end_idx:]

            # 困惑集中指定的词，直接取结果
            if err_type == ErrorType.confusion:
                corrected_item = self.custom_confusion[item]
            else:
                # 对非中文的错字不做处理
                if not is_chinese_string(item):
                    continue
                # 取得所有可能正确的词
                maybe_right_items = self.generate_items(item)
                if not maybe_right_items:
                    continue
                corrected_item = self.lm_correct_item(item, maybe_right_items, before_sent, after_sent)
            # output
            if corrected_item != item:
                sentence = before_sent + corrected_item + after_sent
                # logging.debug('predict:' + item + '=>' + corrected_item)
                detail_word = [item, corrected_item, begin_idx, end_idx]
                detail.append(detail_word)
        detail = sorted(detail, key=operator.itemgetter(2))
        return sentence, detail

if __name__ == "__main__":
    c = Corrector()
    corrected_sent, detail = c.correct('黑龙江大学,学与应用数学')
    print(corrected_sent, detail)
