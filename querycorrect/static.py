import os, traceback, logging, time
from pyspark import SparkContext, SparkConf
from parse_utils import parse_line_querys, parse_line_jd, parse_line_cv, parse_line_jdtitle

def init():
    logging.basicConfig(level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filename='./log',
            filemode='w')

hadoop_input_file = 'hdfs:///basic_data/tob/operation/search_data_jc/*'
#hadoop_input_file = 'hdfs:///basic_data/tob/operation/search_data_jc/20191024/search_data.log'
#hadoop_input_file = 'file:///D:/Python Project/querycorrect/corpus/jddata0'
hadoop_input_file = 'hdfs:///basic_data/jd/positions_extras/20190301/*'
hadoop_input_file = 'hdfs:///basic_data/jd/positions_extras/20190301/position_0/data__092fe84d_2188_49af_a4e4_d954708c08ac'
hadoop_input_file = 'hdfs:///basic_data/jd/positions/20190301/*'
hadoop_input_file = 'hdfs:///basic_data/jd/positions/20190301/position_0/data__133f4412_7fa6_4ee5_8a26_907b9926d7f6'

def static_text(input_file, outputfile):
    try:
        os.system("hadoop fs -rm -r " + outputfile + "_*")
        output_file = outputfile + time.strftime('_%Y-%m-%d_%H_%M_%S',time.localtime(time.time()))
        conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
        sc = SparkContext.getOrCreate(conf)
        lines = sc.textFile(input_file)
        #a = lines.flatMap(parse_line_jdtitle).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).map(lambda x: "%s\t%d" % (x[0], x[1])).collect()
        lines.flatMap(parse_line_jdtitle).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).map(lambda x: "%s&%d" % (x[0], x[1])).saveAsTextFile(output_file)
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
        #a=lines.flatMap(parse_line_querys).map(lambda word:(word,1)).reduceByKey(lambda a, b : a + b).map(lambda x: "%s\t%d" % (x[0], x[1])).collect()
        lines.flatMap(parse_line_querys).map(lambda word:(word,1)).reduceByKey(lambda a, b : a + b).map(lambda x: "%s&%d" % (x[0], x[1])).saveAsTextFile(output_file)
        logging.info(lines.first().split())
        os.system("hadoop fs -get " + output_file + " ../")
    except Exception as e:
        tb = traceback.format_exc();  logging.error('traceback:%s' % str(tb))

if __name__ == '__main__':
    #init()
    #static_query(hadoop_input_file, 'hdfs:///user/kdd_zouning/candidate_query')
    static_text(hadoop_input_file, 'hdfs:///user/kdd_zouning/text')

