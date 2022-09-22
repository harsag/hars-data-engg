import time
from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import *

def read_postgres_pt():
    print('XXX: Creating session')
    this_time = time.time()

    spark = SparkSession \
        .builder \
        .config("spark.jars", "D:\\postgresql-42.4.0.jar") \
        .master("local[*]") \
        .appName("PySpark_Postgres_test") \
        .getOrCreate()

    print('XXX: Spark session started in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    df = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "pol_trans4") \
    .option("user", "postgres").option("password", "admin123") \
    .option("partitionColumn","yyyymm") \
    .option("lowerBound", "202001") \
    .option("upperBound", "202301") \
    .option("numPartitions",4) \
    .load()

    print('XXX: DF Prepared in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()
    
    #df.printSchema()
    #df.show()
    
    df.createOrReplaceTempView("my_trans")
    
    print('XXX: Temp View Prepared in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()
    
    df_final = spark.sql('''select  yyyymm,
        lob, sum(WP) Written, count(distinct pol_no) totalPolicies,
        max(WP) MaxWritten, min(WP) MinWritten
        from    my_trans
        group by yyyymm, lob
        order by yyyymm, lob''')

    print('XXX: Query run in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    #df_final.printSchema()
    #df_final.show()
    
    print('XXX: Printed result ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()
    
    df_final.write.format('jdbc') \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "pol_summ") \
        .option("user", "postgres").option("password", "admin123") \
        .mode("append").save()

    print('XXX: Inserted summary in postgres ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    return

if __name__ == '__main__':
    print('Spark is reading postgres.... ')

    st_time = time.time()
    read_postgres_pt()
    en_time = time.time()

    print('Total time taken was :' + str(en_time-st_time) + ' seconds')
