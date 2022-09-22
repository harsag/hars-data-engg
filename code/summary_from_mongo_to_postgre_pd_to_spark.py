import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pymongo
import pandas as pd

def read_mongo_pd_sp():
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

    myclient = pymongo.MongoClient()
    db = myclient["local"]
    my_collection = db["p_transactions"]

    x = my_collection.find()

    print('XXX: Fetched in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    df1 =  pd.DataFrame(list(x), columns = ['begin_date', 'lob', 'WP', 'pol_no'])

    print('XXX: Pandas DF prepared in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    df=spark.createDataFrame(df1)

    print('XXX: Spark DF Prepared in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    #df.show()
    #print('XXX: Spark DF Displayed in ' + str(time.time() - this_time) + ' seconds')
    #this_time = time.time()

    df.createOrReplaceTempView("my_trans")

    print('XXX: Temp View Prepared in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    df_final = spark.sql('''select  CAST(date_format(begin_date,'yyyyMM') AS INT) yyyymm,
        lob, sum(WP) Written, count(distinct pol_no) totalPolicies,
        max(WP) MaxWritten, min(WP) MinWritten
        from    my_trans
        group by date_format(begin_date,'yyyyMM'), lob
        order by date_format(begin_date,'yyyyMM'), lob''')

    print('XXX: Query run in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    print('Total records in df_final are: ' + str(df_final.count()))

    #df_final.printSchema()
    #df_final.show()
    #print('XXX: Printed result ' + str(time.time() - this_time) + ' seconds')
    #this_time = time.time()

    df_final.write.format('jdbc') \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver").option("dbtable", "pol_summ") \
        .option("user", "postgres").option("password", "admin123") \
        .mode("append").save()

    print('XXX: Inserted summary in postgres ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    return

if __name__ == '__main__':
    print('Pandas is reading mongo and writing the summary to postgres using Spark.... ')
    st_time = time.time()
    read_mongo_pd_sp()
    en_time = time.time()
    print('Total time taken was :' + str(en_time-st_time) + ' seconds')
