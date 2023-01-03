import pyspark
from pyspark.sql import SparkSession
from pyspark.broadcast import Broadcast, BroadcastPickleRegistry
import findspark
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame

import json
import logging
import os
import sys

from pathlib import Path
findspark.init()
from pyspark.conf import SparkConf
spark_conf = SparkConf()
user_home = str(Path.home())
extra_jars = [f"{user_home}/.spark/jars/{jar}" for jar in [
            "hadoop-aws-3.2.3.jar",
            "aws-java-sdk-bundle-1.12.227.jar",
            "postgresql-42.4.0.jar","x12-parser-1.14.jar","editojson-1.0.jar","json-serde.jar"
        ]]

spark_conf.set("spark.jars", ",".join(extra_jars))
spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
#spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType
# Create SparkSession

rddWhole = spark.sparkContext.wholeTextFiles('C:\\Users\\mdkha\\Downloads\\CH_837PS_O_20221217233725.txt')
deptColumns = ["dept_name", "dept_id"]
deptDF = spark.createDataFrame(rddWhole, schema=deptColumns)
deptDF=deptDF.withColumn("ST_LOOPS",F.explode(F.split(F.col('dept_id'),'~ST')))
deptDF.printSchema()
deptDF.show(truncate=False)
isa_loop_df=deptDF.filter(F.col('ST_LOOPS').contains('ISA*00*')).select(F.col('ST_LOOPS'))
isa_loop_str=str(deptDF.collect()[0].ST_LOOPS)
print(isa_loop_str)
isa_loop_str="".join([isa_loop_str,'~ST'])
print(isa_loop_str)
deptDF = deptDF.filter(~F.col('ST_LOOPS').contains('ISA*00*'))
deptDF=deptDF.withColumn('ST_LOOPS',F.concat_ws('',F.lit(isa_loop_str),F.col('ST_LOOPS')))
deptDF.show(truncate=False)
deptDF.select('ST_LOOPS').show(truncate=False)
print(deptDF.count())
spark.udf.registerJavaFunction("edi_to_json", "com.imsweb.x12.EdiToJson", T.StringType())
deptDF=deptDF \
    .withColumn("edi_json", F.expr("edi_to_json(ST_LOOPS)"))
deptDF.select('edi_json').show(truncate=True)


