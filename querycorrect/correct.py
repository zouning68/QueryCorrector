import codecs, operator, time, logging, sys
from pypinyin import lazy_pinyin
import numpy as np
from data_utils import load_char_set, load_same_pinyin, load_same_stroke, load_word_freq_dict
from utils import PUNCTUATION_LIST, uniform, is_chinese_string, edit_distance_word, ErrorType, is_alphabet_string, is_name, need_correct_pinying
from config import config
from seg_utils import Tokenizer
from company import get_query_entity, get_entity

class Detector(Tokenizer):
    def __init__(self, language_model_path=config.language_model_path,
                 word_freq_path=config.word_freq_path, custom_word_freq_path=config.custom_word_freq_path,
                 custom_confusion_path=config.custom_confusion_path, person_name_path=config.person_name_path,
                 place_name_path=config.place_name_path, stopwords_path=config.stopwords_path):
        super(Detector, self).__init__()
        self.name = 'detector'
        self.correct_sentence, self.senten2term, self.query_entitys, self.maybe_errors = "", [], [], []
        self.confusion_sets = ['前段']
        self.word_freq_path = word_freq_path
        self.custom_word_freq_path = custom_word_freq_path
        self.custom_confusion_path = custom_confusion_path
        self.person_name_path = person_name_path
        self.place_name_path = place_name_path
        self.stopwords_path = stopwords_path
        self.is_char_error_detect = True
        self.is_word_error_detect = True
        self.is_confusion_word_error_detect = True
        self.initialized_detector = False
        #initialize detector dict sets
        # 词、频数 dict
        t1 = time.time()
        self.word_freq = load_word_freq_dict(self.word_freq_path, config.word_freq_th)
        t2 = time.time()
        logging.debug('Loaded word freq file: %s, size: %d, spend: %s s' % (self.word_freq_path, len(self.word_freq), str(t2 - t1)))
        # 自定义混淆集
        self.custom_confusion = self._get_custom_confusion_dict(self.custom_confusion_path)
        t3 = time.time()
        logging.debug('Loaded confusion file: %s, size: %d, spend: %s s' % (self.custom_confusion_path, len(self.custom_confusion), str(t3 - t2)))
        # 自定义切词词典
        self.custom_word_freq = load_word_freq_dict(self.custom_word_freq_path)
        self.person_names = load_word_freq_dict(self.person_name_path)
        self.place_names = load_word_freq_dict(self.place_name_path)
        self.stopwords = load_word_freq_dict(self.stopwords_path)
        self.word_freq.update(load_word_freq_dict(config.common_char_path, config.commom_char_th))         # 公共字符集扩充词频字典
        # 合并切词词典及自定义词典
        self.custom_word_freq.update(self.person_names)
        self.custom_word_freq.update(self.place_names)
        self.custom_word_freq.update(self.stopwords)
        self.word_freq.update(self.custom_word_freq)
        t4 = time.time()
        logging.debug('Loaded custom word file: %s, size: %d, spend: %s s' % (self.custom_confusion_path, len(self.custom_word_freq), str(t4 - t3)))
        #self.tokenizer = Tokenizer(dict_path=self.word_freq_path, custom_word_freq_dict=self.custom_word_freq, custom_confusion_dict=self.custom_confusion)
        self.initialized_detector = True

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

    def ngram_score(self, chars):       # 取n元文法得分。chars: list, 以词或字切分
        return self.lm.score(' '.join(chars), bos=False, eos=False)

    def ppl_score(self, words):         # 取语言模型困惑度得分，越小句子越通顺。words: list, 以词或字切分
        return self.lm.perplexity(' '.join(words))

    def word_frequency(self, word):     # 取词在样本中的词频
        return self.word_freq.get(word, 0)

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
        median = np.median(scores);#np.median(scores, axis=0)  # get median of all scores
        margin_median = np.abs(scores - median).flatten()  # deviation from the median
        # 平均绝对离差值
        med_abs_deviation = np.mean(margin_median);#np.median(margin_median)
        if med_abs_deviation == 0:
            return result
        y_score = ratio * margin_median / med_abs_deviation
        # 打平
        scores = scores.flatten()
        maybe_error_indices = np.where((y_score > threshold) & (scores < median))
        # 取全部疑似错误字的index
        result = list(maybe_error_indices[0])
        result = [int(res) for res in result]       # int64 -> int
        return result

    @staticmethod
    def is_filter_token(token):
        result = False
        if not token.strip(): result = True  # pass blank
        if token in PUNCTUATION_LIST: result = True  # pass punctuation
        if token.isdigit(): result = True # pass num
        if is_alphabet_string(token.lower()): result = True  # pass alpha
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
        if need_correct_pinying(sentence): correctpinyin = False
        else: correctpinyin = True
        correct_sentence, senten2term, char_seg, word_seg, detail_eng = self.tokenize(sentence, correct_pinyin=correctpinyin)
        self.correct_sentence = correct_sentence
        self.senten2term = senten2term
        # 1、自定义混淆集 custom_confusion 加入疑似错误词典 maybe_errors
        if self.is_confusion_word_error_detect:
            for confuse in self.custom_confusion:       # 在错误词典 custom_confusion 中的词为纠错词
                idx = correct_sentence.find(confuse)
                if idx > -1:
                    maybe_err = [confuse, idx, idx + len(confuse), ErrorType.confusion]
                    self._add_maybe_error_item(maybe_err, maybe_errors)
        # 2、未登录词加入疑似错误词典 maybe_errors：self.word_freq（word_freq, custom_word_freq, person_name, place_name, stopwords）为正确的词
        if self.is_word_error_detect:
            for word, begin_idx, end_idx in word_seg:
                if self.is_filter_token(word): continue      # pass filter word
                if word in self.word_freq: continue          # pass in dict
                if len(word) == 1: continue             # 单个词过滤
                maybe_err = [word, begin_idx, end_idx, ErrorType.word]
                self._add_maybe_error_item(maybe_err, maybe_errors)
        # 3、语言模型检测疑似错误字
        if self.is_char_error_detect:
            try:
                ngram_avg_scores = []
                for n in [1, 2, 3]:
                    scores = []
                    for i in range(len(senten2term) - n +1):
                        word = senten2term[i: i+n]
                        score = self.ngram_score(list(word))
                        scores.append(score)
                    if not scores:
                        continue
                    # 移动窗口补全得分
                    for _ in range(n - 1):
                        scores.insert(0, scores[0])
                        scores.append(scores[-1])
                    avg_scores = [sum(scores[i:i + n]) / len(scores[i:i + n]) for i in range(len(senten2term))]
                    ngram_avg_scores.append(avg_scores)
                # 取拼接后的n-gram平均得分
                if ngram_avg_scores: sent_scores = list(np.average(np.array(ngram_avg_scores), axis=0))
                else: sent_scores = [0]
                # 取疑似错字信息
                maybe_error_index = self._get_maybe_error_index(sent_scores, threshold=1)
                for i in maybe_error_index:
                    token = senten2term[i]
                    # pass filter word
                    #if self.is_filter_token(token) or token in self.word_freq or token in [e[0] for e in maybe_errors]:
                    if token not in self.confusion_sets and (self.is_filter_token(token) or token in self.word_freq):
                        continue
                    idx = correct_sentence.find(token)
                    maybe_err = [token, idx, idx + len(token), ErrorType.term]  # token, begin_idx, end_idx, error_type
                    self._add_maybe_error_item(maybe_err, maybe_errors)
            except IndexError as ie:
                logging.warning("index error, sentence:" + sentence + str(ie))
            except Exception as e:
                logging.warning("detect error, sentence:" + sentence + str(e))
        return sorted(maybe_errors, key=lambda k: k[1], reverse=False), detail_eng

class Corrector(Detector):
    def __init__(self, common_char_path=config.common_char_path, same_pinyin_path=config.same_pinyin_path,
                 same_stroke_path=config.same_stroke_path, language_model_path=config.language_model_path,
                 word_freq_path=config.word_freq_path, custom_word_freq_path=config.custom_word_freq_path,
                 custom_confusion_path=config.custom_confusion_path, person_name_path=config.person_name_path,
                 place_name_path=config.place_name_path, stopwords_path=config.stopwords_path):
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
        self.cn_char_set = load_char_set(self.common_char_path, config.commom_char_th)  # chinese common char dict
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
        _same_pinyin_, _same_stroke_ = self.get_same_pinyin(c), self.get_same_stroke(c)     # DEBUG
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

    def lm_correct_item(self, item, maybe_right_items, before_sent, after_sent):        # 通过语言模型纠正字词错误
        if item not in maybe_right_items:
            maybe_right_items.append(item)
        perplexitys = []
        _, before_sent_seg, _, _, _ = self.tokenize(before_sent, False)
        _, after_sent_seg, _, _, _ = self.tokenize(after_sent, False)
        for k in maybe_right_items:
            senten2term = before_sent_seg + [k] + after_sent_seg
            perplexity = self.ppl_score(senten2term)
            perplexitys.append((k, perplexity))
            a=1
        sorted_perplexitys = sorted(perplexitys, key=lambda d: d[1])
        corrected_item = sorted_perplexitys[0][0]
        for i, s in sorted_perplexitys:         # 分数相同的情况下取原来的 term
            if i == item and s == sorted_perplexitys[0][1]:
                corrected_item = i
                break
        #corrected_item_ = min(maybe_right_items, key=lambda k: self.ppl_score(list(before_sent + k + after_sent)))
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
        self.maybe_errors, detail_eng = self.detect(sentence)
        # trick: 类似翻译模型，倒序处理
        #maybe_errors = sorted(maybe_errors, key=operator.itemgetter(2), reverse=True)
        self.query_entitys = get_entity(sentence)['info'] #get_query_entity(sentence)['info']
        for item, begin_idx, end_idx, err_type in self.maybe_errors:
            is_contain = self._check_contain_error([item, begin_idx, end_idx, err_type], self.query_entitys)
            if is_contain: continue
            #print(maybe_errors, '\n', query_entitys, '\n', is_contain); exit()
            # 纠错，逐个处理
            before_sent = self.correct_sentence[:begin_idx]
            after_sent = self.correct_sentence[end_idx:]
            # 困惑集中指定的词，直接取结果
            if err_type == ErrorType.confusion:
                corrected_item = self.custom_confusion[item]
            else:
                # 姓名或者对非中文的错字不做处理，已经处理过
                if not is_chinese_string(item) or is_name(item):
                    continue
                # 取得所有可能正确的词：1、相同拼音的词，2、自定义的同义词，3、相同拼音的字
                maybe_right_items = self.generate_items(item)
                if not maybe_right_items:
                    continue
                corrected_item = self.lm_correct_item(item, maybe_right_items, before_sent, after_sent)
            # output
            if corrected_item != item:
                self.correct_sentence = before_sent + corrected_item + after_sent
                # logging.debug('predict:' + item + '=>' + corrected_item)
                detail_word = [item, corrected_item, begin_idx, end_idx]
                detail.append(detail_word)
        detail.extend(detail_eng)
        detail = sorted(detail, key=operator.itemgetter(2))
        return self.correct_sentence, detail

if __name__ == "__main__":
    try: que = sys.argv[1]
    except: que = "pptv,andorid" #"百读jaca开法工成师,后太程序开发"
    c = Corrector()
    corrected_sent, detail = c.correct(que)
    print(que, " ------> " ,corrected_sent, detail)
