import os, jieba
import numpy as np
from keras.models import Model
from keras.layers import Embedding, Dense, Dropout, Input, Conv1D
from keras_contrib.layers import CRF

unk_flag, pad_flag, cls_flag, sep_flag = '[UNK]', '[PAD]', '[CLS]', '[SEP]'
max_len = 20           # 序列的最大长度

current_dir = os.path.dirname(os.path.abspath(__file__))  # 获取当前地址
path_vocab = os.path.join(current_dir, './models/vocab.txt')
model_path = os.path.join(current_dir, './models/IDCNNCRF2.HDF5')

# 获取 word to index 词典
def get_w2i(vocab_path = path_vocab):
    w2i = {}    ;w2i[" "] = 0
    with open(vocab_path, 'r', encoding='utf8') as f:
        while True:
            text = f.readline()
            if not text:
                break
            text = text.strip()
            if text and len(text) > 0:
                w2i[text] = len(w2i) + 1
    return w2i

# 获取 tag to index 词典
def get_tag2index():
    return {"O": 0, "B-SP": 1, "I-SP": 2, "B-SS": 3, "I-SS": 4}  # query 纠错标注的标签

class IDCNNCRF2():
    def __init__(self,
                 vocab_size: int,  # 词的数量(词表的大小)
                 n_class: int,  # 分类的类别(5个类别)
                 max_len: int = 100,  # 最长的句子最长长度
                 embedding_dim: int = 128,  # 词向量编码长度
                 drop_rate: float = 0.5,  # dropout比例
                 ):
        self.vocab_size = vocab_size
        self.n_class = n_class
        self.max_len = max_len
        self.embedding_dim = embedding_dim
        self.drop_rate = drop_rate

    def creat_model(self):
        inputs = Input(shape=(self.max_len,))
        x = Embedding(input_dim=self.vocab_size, output_dim=self.embedding_dim)(inputs)
        x = Conv1D(filters=32, kernel_size=2, activation='relu', padding='same', dilation_rate=1)(x)
        x = Conv1D(filters=64, kernel_size=3, activation='relu', padding='same', dilation_rate=2)(x)
        x = Conv1D(filters=128, kernel_size=4, activation='relu', padding='same', dilation_rate=4)(x)
        x = Dropout(self.drop_rate)(x)
        x = Dense(256)(x)
        x = Dropout(self.drop_rate)(x)
        x = Dense(self.n_class)(x)
        self.crf = CRF(self.n_class, sparse_target=False)
        x = self.crf(x)
        self.model = Model(inputs=inputs, outputs=x)
        return self.model

class nerdetect():
    def __init__(self):
        self.w2i = get_w2i()  # word to index
        self.i2w = {v: k for k, v in self.w2i.items()}
        self.tag2index = get_tag2index()  # tag to index
        self.index2tag = {v: k for k, v in self.tag2index.items()}
        self.vocab_size = len(self.w2i)
        self.tag_size = len(self.tag2index)
        self.unk_flag = unk_flag
        self.unk_index = self.w2i.get(unk_flag, 101)
        self.pad_index = self.w2i.get(pad_flag, 1)
        self.cls_index = self.w2i.get(cls_flag, 102)
        self.sep_index = self.w2i.get(sep_flag, 103)
        model_class = IDCNNCRF2(self.vocab_size, self.tag_size, max_len=max_len)
        self.model = model_class.creat_model()
        self.model.load_weights(model_path)

    def sentence2id(self, sent):
        seg_query = []
        for e in list(jieba.cut(sent)):
            if u'\u4e00' <= e <= u'\u9fa5': seg_query.extend(list(e))
            else: seg_query.append(e)
        ids = [self.w2i.get(w, self.w2i[self.unk_flag]) for w in seg_query]
        if len(ids) < max_len:
            pad_num = max_len - len(ids)
            data_ids = [self.pad_index] * pad_num + ids
        else:
            data_ids = ids[:max_len]
        return data_ids, len(seg_query)

    def detect(self, sentence):
        res, error = [], []
        data_ids, sent_len = self.sentence2id(sentence)
        sent_len = [sent_len]
        sentids = np.array([data_ids])
        pre = self.model.predict(sentids)
        probs = np.argmax(pre, axis=2)
        for sent in range(len(sentids)):
            pred = list(probs[sent][-sent_len[sent]:])
            sentce = list(sentids[sent][-sent_len[sent]:])
            for word in range(sent_len[sent]):
                res.append((self.i2w.get(sentce[word]), self.index2tag.get(pred[word])))
        entity_name, entity_start = "", 0
        for i in range(len(res)):
            word, tag = res[i]
            if word == self.unk_flag: continue
            if tag[0] == "B" or tag[0] == "I":
                entity_name += word
                error_type = tag
            if (tag[0] == "O" or i == len(res)-1) and entity_name:
                entity_start = sentence.find(entity_name)
                error.append([entity_name, entity_start, entity_start + len(entity_name), error_type.split("-")[-1]])
                entity_name = ""
        return res, error

if __name__ == "__main__":
    nd = nerdetect()  # IDCNNCRF2
    print(nd.detect("运车管家"))