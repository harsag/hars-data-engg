import time
from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import *

def read_mongo():
    print('XXX: Creating session')
    this_time = time.time()

    spark = SparkSession \
        .builder \
        .config("spark.jars", "D:\\postgresql-42.4.0.jar") \
        .master("local[*]") \
        .appName("PySpark_Postgres_test") \
        .getOrCreate()

    print('XXX: Spark session started in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    df = spark.read\
        .format('com.mongodb.spark.sql.DefaultSource')\
        .option( "uri", "mongodb://127.0.0.1:27017/local.p_transactions") \
        .load().select('begin_date', 'lob', 'WP', 'pol_no')

    # df.show()

    print('XXX: DF Prepared in Spark in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()
	
    df.createOrReplaceTempView("my_trans")

    print('XXX: Temp View Prepared in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()
    
    df_final = spark.sql('''select  CAST(date_format(begin_date,'yyyyMM') as INT) yyyymm,
        lob, sum(WP) Written, count(distinct pol_no) totalPolicies,
        max(WP) MaxWritten, min(WP) MinWritten
        from    my_trans
        group by date_format(begin_date,'yyyyMM'), lob
        order by date_format(begin_date,'yyyyMM'), lob''')

    print('XXX: Query run in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    # df_final.printSchema()
    # df_final.show()
    print('Total records in df_final are: ' + str(df_final.count()))

    # print('XXX: Printed result ' + str(time.time() - this_time) + ' seconds')
    # this_time = time.time()
    
    df_final.write.format('jdbc') \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "pol_summ") \
        .option("user", "postgres").option("password", "admin123") \
        .mode("append").save()

    print('XXX: Inserted summary in postgres ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    return

if __name__ == '__main__':
    print('Spark is reading mongo and writing the summary to postgres.... ')
    st_time = time.time()
    read_mongo()
    en_time = time.time()
    print('Total time taken was :' + str(en_time-st_time) + ' seconds')
