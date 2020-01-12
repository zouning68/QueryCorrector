# 压缩项目文件
rm static_spark.zip
zip -r static_spark.zip *

# python.zip需要提前上传到hdfs中
hdfs_py_path="hdfs:///user/kdd_zouning/test/conda_test_py3.zip"
executor_dir="anaconda3"
# 本地的python路径
driver_python="/opt/userhome/kdd_zouning/anaconda3/bin/python3"
executor_python="anaconda3/bin/python3"
exenum=4

echo ${pwd}

#--conf spark.yarn.dist.archives="${hdfs_py_path}#$executor_dir"\
spark-submit \
    --conf spark.pyspark.driver.python=$driver_python \
    --conf spark.pyspark.python=$executor_python \
    --conf spark.yarn.dist.archives="${hdfs_py_path}#$executor_dir"\
    --master local \
    --deploy-mode client \
    --executor-memory 1G \
    --num-executors $exenum \
    --executor-cores 4 \
    --py-files static_spark.zip \
    static.py

rm static_spark.zip

exit $?
