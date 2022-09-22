import time
from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import *

def save_to_postgres(st, ed):
    print('XXX: Preparing Files list')
    this_time = time.time()
    
    files_names = ['D:\\bigdata\\bigdata-perf\\input\\tr' + str(fn) + '.json' for fn in range(st, ed)]
    #print(files_names)

    print('XXX: File list prepared in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    spark = SparkSession \
        .builder \
        .config("spark.jars", "D:\\postgresql-42.4.0.jar") \
        .master("local[*]") \
        .appName("PySpark_Postgres_test") \
        .getOrCreate()

    print('XXX: Spark session started in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    df = spark.read.json(files_names)
    df.show()
    
    df1 = df.withColumn('begin_date', to_date(df.begin_date,"yyyy-MM-dd")) \
        .withColumn('cancel_date', to_date(df.cancel_date,"yyyy-MM-dd")) \
        .withColumn('end_date', to_date(df.end_date,"yyyy-MM-dd")) \
        .withColumn('inception_date', to_date(df.inception_date,"yyyy-MM-dd"))
        
    #df1.show()
    #df1.printSchema()

    print('XXX: DF Prepared in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    df1.write.format('jdbc') \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "pol_trans") \
        .option("user", "postgres").option("password", "admin123") \
        .mode("append").save()

    print('XXX: Inserted in postgres ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    return

if __name__ == '__main__':
    # print('Enter start of range')
    # st = int(input())
    # print('Enter count')
    # no_of_files = int(input())
    st = int(sys.argv[1])
    no_of_files = int(sys.argv[2])
    print('Spark is saving files to postgres.... ')

    st_time = time.time()
    save_to_postgres(st, st + no_of_files)
    en_time = time.time()

    print('Total time taken for ' + str(no_of_files) + ' files was :' + str(en_time-st_time) + ' seconds')
    print('Last file name was ' + str(st + no_of_files - 1))
