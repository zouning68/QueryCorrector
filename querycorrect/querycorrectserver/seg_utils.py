import logging, json, requests, jieba, sys, os, copy, re
#from nlutools import tools as nlu
from english_corrector import EnglishCorrector
from data_utils import load_word_freq_dict, _get_custom_confusion_dict
from utils import re_en, re_salary, is_alphabet_string, pinyin2hanzi, ErrorType, PUNCTUATION_LIST
from config import config

re_seg = re.compile(u"([，,])", re.S)
def regular_cut(text):
    return re_seg.split(text)
a=re_seg.split("董英姿,前端")

def cut(text):
    res = []
    try:
        url = 'http://192.168.12.18:51990/huqie'
        body = {"txt":str(text)}
        query = json.dumps(body)
        headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
        response = requests.post(url, data=query, headers=headers)
        response = json.loads(response.text)
        res = response['1']['txt']
    except Exception as e:
        logging.warn('getSegTxt_error=%s' % (str(repr(e))))
    return res

def jieba_cut(text):
    res = list(jieba.cut(text))
    return res

def nlu_cut(text):
    res = nlu.cut(text)
    return res

class Tokenizer(EnglishCorrector):
    def __init__(self):
        super(Tokenizer, self).__init__()
        self.model = jieba
        self.model.default_logger.setLevel(logging.ERROR)
        self.use_word_freq_dict = True
        # 初始化大词典
        if os.path.exists(config.word_freq_path) and self.use_word_freq_dict:
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
        self.token_word_english = load_word_freq_dict(config.english_path)
        self.token_word_english.update(load_word_freq_dict(config.word_freq_path))
        self.pinying = load_word_freq_dict(config.pingying)
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

    def tokenize(self, sentence, correct_eng=True, correct_pinyin=False):
        correct_sentence, senten2term, char_seg, word_seg, detail_eng, char_index, word_index = '', [], [], [], [], 0, 0
        #a=re_en.split(sentence)#; sentence = "上海百度公司java,elastic开法工程师"; aa=list(self.model.tokenize(sentence))
        for word in re_en.split(sentence):
            word = word.strip().lower()
            if word in ['', ' ']: continue
            if re_en.fullmatch(word):   # 英文处理
                #a = wordnet.synsets(word)
                if correct_pinyin and len(word) > 2 and (word not in self.token_word_english or word in self.pinying):
                    hanzi = pinyin2hanzi([word], 1)       # 拼音转汉字处理
                else: hanzi = []
                if word in self.custom_filter_word: rword = word    # 优先考虑自定义字典
                elif hanzi: rword = hanzi[0].path[0]                   # 拼音转汉字处理
                elif re_salary.fullmatch(word): rword = word            # 薪水term处理
                elif correct_eng: rword = self.correction(word)     # 英文纠错
                else: rword = word                                  # 不处理
                if config.char_term:
                    char_seg.append((rword, char_index, char_index + len(rword)))
                    char_index += len(rword)
                else:
                    word_seg.append((rword, word_index, word_index+len(rword)))
                    word_index +=len(rword)
                if rword != word:       # 记录英文纠错细节
                    detail_eng.append([word, rword, char_index, char_index+len(rword), ErrorType.english])
            else:                       # 非英文处理
                if config.char_term:    # 字符级别
                    for w in list(word):
                        char_seg.append((w, char_index, char_index + 1))
                        char_index += 1
                else:                   # 分词级别
                    model_seg = list(self.model.tokenize(word))
                    word_seg.extend([(e[0], e[1]+word_index, e[2]+word_index) for e in model_seg])
                    word_index = word_seg[-1][2]
        if config.char_term: senten2term = [e[0] for e in char_seg]
        else: senten2term = [e[0] for e in word_seg]
        for i in range(len(senten2term)):
            if i < len(senten2term) - 1 and is_alphabet_string(senten2term[i]) and  is_alphabet_string(senten2term[i+1]):
                correct_sentence += senten2term[i] + ' '
            else:
                correct_sentence += senten2term[i]
        return correct_sentence, senten2term, char_seg, word_seg, detail_eng

if __name__ == '__main__':
    try: que = sys.argv[1]
    except: que = "开法工程师"
    #nlu_seg = nlu_cut(que)
    jieba_seg = jieba_cut(que)
    #print(json.dumps(cut(que), ensure_ascii=False))   # 分词服务
    t = Tokenizer(); a = t.tokenize("客服助理(,宁波)(6243)", True)
    pass

