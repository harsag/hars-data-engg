import time
from pyspark.sql import SparkSession
import sys

def save_to_mongo(st, ed):
    print('XXX: Preparing Files list')
    this_time = time.time()
    
    files_names = ['D:\\bigdata\\bigdata-perf\\input\\tr' + str(fn) + '.json' for fn in range(st, ed)]
    #print(files_names)

    print('XXX: File list prepared in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    spark = SparkSession \
        .builder \
        .appName("mongodbtest1") \
        .master('local[*]') \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/local.p_transactions2") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/local.p_transactions2") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    print('XXX: Spark session started in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    df = spark.read.json(files_names)
    #df.show()

    print('XXX: DF Prepared in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    df.write.format('com.mongodb.spark.sql.DefaultSource') \
        .option("uri", "mongodb://127.0.0.1:27017/local.p_transactions2") \
        .mode("append").save()

    print('XXX: Inserted in mongo ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    return

if __name__ == '__main__':
    # print('Enter start of range')
    # st = int(input())
    # print('Enter count')
    # no_of_files = int(input())
    st = int(sys.argv[1])
    no_of_files = int(sys.argv[2])
    print('Spark is saving files to mongodb.... ')

    st_time = time.time()
    save_to_mongo(st, st + no_of_files)
    en_time = time.time()

    print('Total time taken for ' + str(no_of_files) + ' files was :' + str(en_time-st_time) + ' seconds')
    print('Last file name was ' + str(st + no_of_files - 1))
