import pyspark.sql.functions as f 
from pyspark import SparkContext, HiveContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DataType, IntegerType, DoubleType, LongType, FloatType, NullType
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sqlwindow import Window
import commands


def main():
    
    #Loading a file to hdfs to be consumed by spark
    commands.getoutput('hadoop fs -put file_name.txt hdfs/path/file_directory')

    #Loading text file into a pyspark DataFrame
    fileDF = sc.textFile("hdfs/path/file_directory").map(lambda x:x.split("\t")).toDF()

    #Rename columns
    renamed_col = fileDF.withColumnRenamed("_1","member_id").withColumnRenamed("_2","FName")

    #Select data based on condition
    filtered_DF = renamed_col.where(renamed_col.member_id == '1234')

    #Select columns and drop duplicates 
    selected_cols = filtered_DF.select(f.col('member_id'),f.col('FName').alias('First_Name')).dropDuplicates()

    #This function union DataFrames
    one_dataframe = firstDF.unionAll(secondDF)

    #Joining DataFrames
    final_DF = firstDF.alias('a').join(secondDF.alias('b'),f.trim(f.col('a.member_id')) == f.trim(f.col('b.member_id')),how='inner')

    mem_list = ['12345','56789']

    #Another way to filter data in a column
    df=final_DF.withColumn("member_id",f.when(final_DF.member_id.isin(mem_list)))


    #Executing HIVE queries in pyspark
    hive_df = hc.sql("DROP TABLE IF EXISTS prod_c_files.Table_name")
    hive_df = hc.sql("""CREATE TABLE prod_c_files.Table_name AS
                        SELECT DISTINCT
                        member_id,
                        first_name
                        FROM prod_c_files.table_example""")


    #Union a list of multiple DataFrames
    dfs = [df1,df2,df3,df4]
    dfs_union = reduce(f.DataFrame.unionAll,dfs).dropDuplicates().orderBy('member_id')

    #Add an extra column with hard code ID
    df_added_col = dfs_union.withColumn("ID",f.lit(717171))

    #Save the data to hdfs
    df_added_col.rdd.map(lambda x: ",".join(map(str, x))).saveAsTextFile('hdfs/path/directory/files/final_output_file')

    #Merging all the files into one single file
    command.getoutput('hadoop fs -getmerge hdfs/path/directory/files/final_output_file The_output_file_name.csv')

    #Deliting files from hdfs path
    commands.getoutput('hadoop fs -rm -f hdfs/path/directory/files/*')
    commands.getoutput('hadoop fs -rmdir hdfs/path/directory/files')
    commands.getoutput('hadoop fs -rm -r hdfs/path/directory/files/*')



if __name__=="__name__":
    conf = (SparkConf()\
    .set("spark.shuffle.service.enabled","True")\
    .set("spark.dynamicAllocation.enabled","True")\    
    .set("spark.dynamicAllocation.executorIdeltimeout","2m")\
    .set("spark.dynamicAllocation.minExecutors","1")\
    .set("spark.dynamicAllocation.maxExecutors","2000")\    
    .set("sparkdriver.maxResultSize","5G")\
    .set("spark.default.parallelism","900")\
    .set("spark.dynamicAllocation.enabled","True")\
    .set("spark.yarn.executor.memoryOverhead","2G")\
    .set("spark.driver.overhead.memory","2G")\
    .set("spark.executor.overhead.memory","2G")\
    .set("spark.shuffle.memoryFraction","0.0")\
    .set("spark.dynamicAllocation.schedulerBacklogTimeout","5")\
    .set("spark.kryoserializer.buffer.max","1G")
    )

    spark = SparkSession.builder.config(conf=conf).appName("Application Name").enableHiveSupport().getOrCreate()
    sc = spark.SparkContext
    hc = spark

    spark.sql(""" add jar /opt/voltage/lib/vibesimplejava.jar""")

    main()

    sc.stop

