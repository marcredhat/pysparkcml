from pyspark.sql import SparkSession

import os,sys
import os.path
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.files import SparkFiles
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


exampleDir = os.path.join(os.environ["SPARK_HOME"], "/runtime-addons/spark311-13-hf2-jtro7/opt/spark/jars/")
exampleJars = [os.path.join(exampleDir, x) for x in os.listdir(exampleDir)]

handle = SparkSession\
  .builder\
  .appName("test")\
  .config("spark.sql.hive.hwc.execution.mode","spark")\
  .config("spark.sql.extensions","com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension")\
  .config("spark.kryo.registrator","com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator")\
  .config("spark.yarn.access.hadoopFileSystems", "s3a://eng-ml-dev-env-aws/eng-ml-dev-env-aws-dl/warehouse/tablespace/external/hive")\
  .config("spark.jars", ",".join(exampleJars))\
  .config("spark.jars","/tmp/hive-jdbc-handler.jar")\
  .getOrCreate()


EXAMPLE_SQL_QUERY = "show databases"
handle.sql(EXAMPLE_SQL_QUERY).show()

EXAMPLE_SQL_QUERY = "show tables from sys"
handle.sql(EXAMPLE_SQL_QUERY).show()


EXAMPLE_SQL_QUERY = "select   *     FROM `sys`.`TBLS` `T` JOIN `sys`.`DBS` `D` ON (`d`.`db_id` = `t`.`db_id`)"

handle.sql(EXAMPLE_SQL_QUERY).show()

handle.stop()
