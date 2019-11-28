import os, codecs, logging, jieba, traceback

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

def load_word_freq_dict(path, th=0):      # 加载切词词典
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
            if freq < th: continue
            word_freq[word] = freq
    return word_freq

def load_char_set(path, th=0):
    words = set()
    if not os.path.exists(path):
        logging.warning("file not exists:" + path)
        return words
    with codecs.open(path, 'r', encoding='utf-8') as fin:
        for line in fin:
            w, f = line.strip().split()
            if int(f) < th: continue
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
