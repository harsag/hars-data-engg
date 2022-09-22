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
    .option("driver", "org.postgresql.Driver") \
    .option("user", "postgres").option("password", "admin123") \
    .option("dbtable", '(select yyyymm, lob, sum("WP") Written, count(distinct pol_no) totalPolicies, max("WP") MaxWritten, min("WP") MinWritten from pol_trans4 group by yyyymm, lob order by yyyymm, lob) as subq') \
    .option("partitionColumn","yyyymm") \
    .option("lowerBound", "202001") \
    .option("upperBound", "202301") \
    .option("numPartitions",4) \
    .load()

    print('XXX: DF Prepared in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()
    
    df.write.format('jdbc') \
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
