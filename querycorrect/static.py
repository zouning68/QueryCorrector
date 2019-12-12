import os, traceback, logging, time, re, sys
from pyspark import SparkContext, SparkConf
from parse_utils import parse_line_querys, parse_line_jd, parse_line_cv, parse_line_jdtitle, \
    parse_cv_algo, parse_jd_algo

def init():
    logging.basicConfig(level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filename='./log',
            filemode='w')

hadoop_input_file = 'hdfs:///basic_data/tob/operation/search_data_jc/*'     # 搜索日志
#hadoop_input_file = 'hdfs:///basic_data/tob/operation/search_data_jc/20191024/search_data.log'
#hadoop_input_file = 'file:///D:/Python Project/querycorrect/corpus/jddata0'
hadoop_input_file = 'hdfs:///basic_data/jd/positions_extras/20190301/*'    # jd 描述
hadoop_input_file = 'hdfs:///basic_data/jd/positions_extras/20190301/position_0/data__092fe84d_2188_49af_a4e4_d954708c08ac'
#hadoop_input_file = 'hdfs:///basic_data/jd/positions/20190301/*'           # jd 标题
#hadoop_input_file = 'hdfs:///basic_data/jd/positions/20190301/position_0/data__133f4412_7fa6_4ee5_8a26_907b9926d7f6'
hadoop_input_file = 'hdfs:///basic_data/icdc/algorithms/20190907/*'         # cv简历算法字段
#hadoop_input_file = 'hdfs:///basic_data/icdc/algorithms/20190907/icdc_25/data__939261ad_9bac_4e67_9d9b_ecc2b0cb1dec'
#hadoop_input_file = 'hdfs:///basic_data/jd/positions_algorithms/20190301/*' # jd职位算法字段
#hadoop_input_file = 'hdfs:///basic_data/jd/positions_algorithms/20190301/position_9/data__1d7404f1_af82_4282_9b8c_0b467ccc9a95'
hadoop_input_file = 'file:///D:/Python Project/querycorrect/corpus/jdalgo0'

parse_function = parse_jd_algo  #parse_line_jdtitle, parse_line_jd

SPK_CONF = SparkConf()\
        .setAppName("zn-%s"%re.sub(r".*/", "", sys.argv[0]))\
        .set('spark.driver.memory', '20g')\
        .set('spark.executor.memory', '20g')\
        .set('spark.driver.maxResultSize', '100g')\
        .set("spark.hadoop.validateOutputSpecs", "false") \
        .set('spark.executor.extraJavaOptions','-XX:MaxDirectMemorySize=10g')

def static_text(input_file, outputfile):
    try:
        os.system("hadoop fs -rm -r " + outputfile + "_*")
        output_file = outputfile + time.strftime('_%Y-%m-%d_%H_%M_%S',time.localtime(time.time()))
        #conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
        #sc = SparkContext.getOrCreate(conf)
        sc = SparkContext(conf=SPK_CONF)
        lines = sc.textFile(input_file)
        #a = lines.flatMap(parse_function).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False).map(lambda x: "%s\t%d" % (x[0], x[1])).collect()
        lines.flatMap(parse_function).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], False).map(lambda x: "%s&%d" % (x[0], x[1])).saveAsTextFile(output_file)
        logging.info(lines.first().split())
        os.system("hadoop fs -get " + output_file + " ./")
    except Exception as e:
        tb = traceback.format_exc();  logging.error('traceback:%s' % str(tb))

def static_query(input_file, outputfile):
    try:
        os.system("hadoop fs -rm -r " + outputfile + "_*")
        output_file = outputfile + time.strftime('_%Y-%m-%d_%H_%M_%S',time.localtime(time.time()))
        #conf = SparkConf().setAppName("miniProject").setMaster("local[*]")
        #sc = SparkContext.getOrCreate(conf)
        sc = SparkContext(conf=SPK_CONF)
        lines = sc.textFile(input_file)
        #a=lines.flatMap(parse_line_querys).map(lambda word:(word,1)).reduceByKey(lambda a, b : a + b).sortBy(lambda x: x[1], False).map(lambda x: "%s\t%d" % (x[0], x[1])).collect()
        lines.flatMap(parse_line_querys).map(lambda word:(word,1)).reduceByKey(lambda a, b : a + b).sortBy(lambda x: x[1], False).map(lambda x: "%s&%d" % (x[0], x[1])).saveAsTextFile(output_file)
        logging.info(lines.first().split())
        os.system("hadoop fs -get " + output_file + " ./")
    except Exception as e:
        tb = traceback.format_exc();  logging.error('traceback:%s' % str(tb))

if __name__ == '__main__':
    #init()
#    static_query(hadoop_input_file, 'hdfs:///user/kdd_zouning/candidate_query')
#    static_text(hadoop_input_file, 'hdfs:///user/kdd_zouning/jdtitle')
    static_text(hadoop_input_file, 'hdfs:///user/kdd_zouning/cvalgo')

