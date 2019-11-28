import re, logging, traceback
from collections import Counter
from config import config
from Pinyin2Hanzi import DefaultDagParams, dag

#**********************************************************************************************************************#
class ErrorType(object):
    confusion, word, term, english = 'confusion', 'word', 'term', 'english'

dagParams = DefaultDagParams()
def pinyin2hanzi(pinyinList, num=3):
    result = dag(dagParams, pinyinList, path_num=num)
    return result
a = pinyin2hanzi(["meitu"])

names = [e.strip() for e in open(config.baijiaxing, encoding='utf8').readlines() if e.strip() != '']
def is_name(text):
    text = str(text)    ; aa=text[0]
    if len(text) > 2 and text[:2] in names: return True
    if len(text) in [1, 2, 3] and text[0] in names: return True
    else: return False
a=is_name("锜晓敏")

PUNCTUATION_LIST = ".。,，,、?？:：;；{}[]【】“‘’”《》/!！%……（）<>@#$~^￥%&*\"\'=+-_——「」"
re_ch = re.compile(u"([\u4e00-\u9fa5])",re.S)
re_en = re.compile(u"([a-zA-Z\#]+|[0-9]+k[\+]*|c\+\+)",re.S)
re_salary = re.compile(u"([0-9]+k[\+]*)",re.S)

a=re_en.split("montage+深圳c++")

def is_alphabet_string(string):     # 判断是否全部为英文字母
    string = string.lower()
    for c in string:
        if c < 'a' or c > 'z':
            return False
    return True

def need_correct_pinying(string):     # 判断是否全部为英文字母
    string = string.lower()
    for c in string:
        if c in PUNCTUATION_LIST: continue
        if c < 'a' or c > 'z':
            return False
    return True
a=is_alphabet_string("Java,")

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

def edit_distance_word(word, char_set):     # all edits that are one edit away from 'word'
    splits = [(word[:i], word[i:]) for i in range(len(word) + 1)]
    transposes = [L + R[1] + R[0] + R[2:] for L, R in splits if len(R) > 1]
    replaces = [L + c + R[1:] for L, R in splits if R for c in char_set]
    return set(transposes + replaces)
#**********************************************************************************************************************#

def n_gram_words(text, n_gram=4, return_list=False):
    # n_gram 句子的词频字典
    words, words_freq = [], dict()
    try:
        # 1 - n gram
        for i in range(1, n_gram + 1):
            words += [text[j: j + i] for j in range(len(text) - i + 1)]
        words_freq = dict(Counter(words))
    except Exception as e:
        logging.warning('n_gram_words_err=%s' % repr(e)); print(traceback.format_exc())
    if return_list: return words
    else: return words_freq

def rmPunct(line):
    line = re.sub(r"[★\n-•／［…\t」＋＆　➕＊]+", "", line)
    line = re.sub(r"[,\./;'\[\]`!@#$%\^&\*\(\)=\+<> \?:\"\{\}-]+", "", line)
    line = re.sub(r"[、\|，。《》；“”‘’；【】￥！？（）： ～]+", "", line)
    line = re.sub(r"[~/'\"\(\)\^\.\*\[\]\?\\]+", "", line)
    return line
a=rmPunct("消费者/顾客word、excel、ppt、visio、xmind")

def clean_query(query):
    query = re.sub(r"[&$￥～�|＠？＞＝＜；!｜｛＼］［／－＋＊*＆％＃＂！🌐．﹒海金]{1,}|[.#-]{2,}|[+]{3,}|[0-9]*%", "", query)
    query = re.sub(r"[\\/、，]+", ",", query)
    query = re.sub(r"[（]+", "(", query)
    query = re.sub(r"[）]+", ")", query)
    query = re.sub(r"[【】●|“”^H*]+", " ", query)
    query = re.sub(r"[：]+", ":", query)
    query = re.sub(r"[ ~]+", " ", query)
    query = query.lstrip(",")
    query = query.rstrip(",")
    query = query.strip().lower()
    return query
aa=clean_query("####文旅地.......产集｜团.＼.....")

def normal_qeury(text):
    re_ch = re.compile("([\u4e00-\u9fa5])", re.S)
    re_digital = re.compile("[0-9]{2,}", re.S)
    re_longdigital = re.compile("[0-9]{5,}", re.S)
    re_valid = re.compile("[简历]", re.S)
    digital = re_digital.findall(text)
    chinese = re_ch.findall(text)
    valid = re_valid.findall(text)
    long_digital = re_longdigital.findall(text)
    if long_digital: return False
    elif digital and not chinese: return False
    elif len(text) > 20 or len(text) < 2 or valid: return False
    else: return True
a=normal_qeury(clean_query("搜狐畅游17173"))

if __name__ == '__main__':
    a = normal_qeury("k12d2d2")
    A = clean_query("市场销售^H*..")
    pass