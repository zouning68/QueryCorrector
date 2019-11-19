import os, sys, json, traceback, logging, time
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import SparkSession
from utils import n_gram_words, parse_line_querys, parse_line_ngrams

def init():
    logging.basicConfig(level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filename='./log',
            filemode='w')

hadoop_input_file = 'hdfs:///basic_data/tob/operation/search_data_jc/*'
#hadoop_input_file = 'hdfs:///basic_data/tob/operation/search_data_jc/20191024/search_data.log'
hadoop_input_file = 'file:///D:/Python Project/querycorrect/data/search_data.log'

def parse_line(line):
    line = line.strip().split()
    query = line[5].strip().replace(' ', '')
    ngw = n_gram_words(query, 4, True)
    return ngw

def parse_line_(line):
    line = line.strip().split()
    query = [line[5].strip().replace(' ', '')]
    return query

def static_ngram(input_file, output_file):
    try:
        os.system("hadoop fs -rm -r " + output_file)
        conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
        sc = SparkContext.getOrCreate(conf)
        lines = sc.textFile(input_file)
        #a=lines.flatMap(parse_line_ngrams).map(lambda word:(word,1)).reduceByKey(lambda a, b : a + b).map(lambda x: "%s\t%d" % (x[0], x[1])).collect()
        lines.flatMap(parse_line_ngrams).map(lambda word:(word,1)).reduceByKey(lambda a, b : a + b).map(lambda x: "%s&%d" % (x[0], x[1])).saveAsTextFile(output_file)
        logging.info(lines.first().split())
        os.system("hadoop fs -get " + output_file + " ../")
    except Exception as e:
        tb = traceback.format_exc();  logging.error('traceback:%s' % str(tb))

def static_query(input_file, outputfile):
    try:
        os.system("hadoop fs -rm -r " + outputfile + "_*")
        output_file = outputfile + time.strftime('_%Y-%m-%d_%H_%M_%S',time.localtime(time.time()))
        conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
        sc = SparkContext.getOrCreate(conf)
        lines = sc.textFile(input_file)
        a=lines.flatMap(parse_line_querys).map(lambda word:(word,1)).reduceByKey(lambda a, b : a + b).map(lambda x: "%s\t%d" % (x[0], x[1])).collect()
        lines.flatMap(parse_line_querys).map(lambda word:(word,1)).reduceByKey(lambda a, b : a + b).map(lambda x: "%s&%d" % (x[0], x[1])).saveAsTextFile(output_file)
        logging.info(lines.first().split())
        os.system("hadoop fs -get " + output_file + " ../")
    except Exception as e:
        tb = traceback.format_exc();  logging.error('traceback:%s' % str(tb))

if __name__ == '__main__':
    #init()
    static_query(hadoop_input_file, 'hdfs:///user/kdd_zouning/candidate_query')
    #static_ngram(hadoop_input_file, 'hdfs:///user/kdd_zouning/ngram_query')

