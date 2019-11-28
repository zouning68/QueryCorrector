linux: pip install https://github.com/kpu/kenlm/archive/master.zip
windows: pip install -e git+https://github.com/kpu/kenlm.git#egg=kenlm
generate corpus: use_word_freq_dict->False; predict: use_word_freq_dict->True