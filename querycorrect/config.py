import os, time, kenlm, logging

pwd_path = os.path.abspath(os.path.dirname(__file__))
class Config:
    def __init__(self):
        self.eng_language_model_path = os.path.join(pwd_path, 'arpa/eng.5gram.arpa')  # language model path
        # self.language_model_path = os.path.join(pwd_path, 'dict/kenlm/people_chars_lm.klm')  # language model path
        self.word_freq_path = os.path.join(pwd_path, 'dict/word_freq.txt')  # 通用分词词典文件  format: 词语 词频
        self.common_char_path = os.path.join(pwd_path, 'dict/common_char_set.txt')   # 中文常用字符集
        self.same_pinyin_path = os.path.join(pwd_path, 'dict/same_pinyin.txt')   # 同音字
        self.same_stroke_path = os.path.join(pwd_path, 'dict/same_stroke.txt')   # 形似字
        self.custom_confusion_path = os.path.join(pwd_path, 'dict/custom_confusion.txt') # 用户自定义错别字混淆集 format:变体 本体 本体词词频（可省略）
        self.custom_word_freq_path = os.path.join(pwd_path, 'dict/custom_word_freq.txt') # 用户自定义分词词典  format: 词语 词频
        self.person_name_path = os.path.join(pwd_path, 'dict/person_name.txt')   # 知名人名词典 format: 词语 词频
        self.place_name_path = os.path.join(pwd_path, 'dict/place_name.txt') # 地名词典 format: 词语 词频
        self.stopwords_path = os.path.join(pwd_path, 'dict/stopwords.txt')   # 停用词
        self.english_path = os.path.join(pwd_path, 'dict/english.txt')   # 已知的英文字典
        self.char_term = False     # 字或词级别的 term
        self.original_query_path = os.path.join(pwd_path, 'corpus/querys')   # 原始的query文件
        self.baijiaxing = os.path.join(pwd_path, 'dict/baijiaxing.txt')         # 百家姓判断人名
        self.candidate_query_path = os.path.join(pwd_path, 'data/candidate_query')  # 出来后的的query文件
        self.query_corpus_en = os.path.join(pwd_path, 'data/query_corpus_en')  # 用于训练语言模型的英文query文件
        self.kg_nodes_full_data = os.path.join(pwd_path, 'corpus/kg_nodes_full_data_20190916.jsonl')    # 已经挖掘好的实体信息
        self.search_data = os.path.join(pwd_path, 'corpus/sort_search_data')  # 搜索日志
        self.query = os.path.join(pwd_path, 'data/query_original')          # 保存的query
        self.jd_title = os.path.join(pwd_path, 'corpus/jdtitle')            # 职位的title
        self.pingying = os.path.join(pwd_path, 'dict/pinping.txt')
        self.commom_char_th = 1000000
        self.english_th = -2
        self.word_freq_th = -3
        if self.char_term:
            self.language_model_path = os.path.join(pwd_path, 'arpa/query_char.5gram.arpa')  # 字类型的 language model path
            self.query_corpus_ch = os.path.join(pwd_path, 'data/query_corpus_ch_char')  # 用于训练语言模型的中英query文件
        else:
            self.language_model_path = os.path.join(pwd_path, 'arpa/query_word.5gram.arpa')  # 词类型的 language model path
            self.query_corpus_ch = os.path.join(pwd_path, 'data/query_corpus_ch_word')  # 用于训练语言模型的中英query文件
config = Config()
#config.language_model_path = os.path.join(pwd_path, 'ngram-lm-master/arpa/query_word.3gram.arpa')       # TEST

class LanguageModel(object):
    def __init__(self):
        self.name = 'language model'
        t1 = time.time()
        try:
            self.eng_lm = kenlm.Model(config.eng_language_model_path)
            self.lm = kenlm.Model(config.language_model_path)
            #self.eng_lm, self.lm = None, None
        except:
            self.eng_lm, self.lm = None, None
        logging.debug('Loaded language model: %s, spend: %s s' % (config.language_model_path, str(time.time() - t1)))

if __name__ == "__main__":
    chinese, english = "有限公司", "java"
    lanmo = LanguageModel()
    print("\nchinese: %s\tscore: %f\tperplexity: %f" % (chinese, lanmo.lm.score(' '.join(list(chinese))), lanmo.lm.perplexity(' '.join(list(chinese)))))
    print("english: %s\tscore: %f\tperplexity: %f" % (english, lanmo.eng_lm.score(' '.join(list(english))), lanmo.eng_lm.perplexity(' '.join(list(english)))))
