import time
import pymongo
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def read_mongo_pd():
    print('XXX: Creating db connection')
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

    # print(df1.head())
    print('Total records in df1 are: ' + str(len(df1)))

    df1['yyyymm'] = df1['begin_date'].apply(lambda x: x[0:4]+x[5:7]).astype(int)

    df2 = df1.groupby(['yyyymm','lob']).agg({'WP': ['sum', 'max', 'min'], 'pol_no': 'nunique'})
    df2.columns = ['Written', 'MaxWritten', 'MinWritten', 'totalPolicies']
    df2.reset_index(inplace=True)

    print('XXX: Pandas DF transformed in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    print('Total records in df2 are: ' + str(len(df2)))
    # print(df2.head())

    # Write to postgres

    # establish connections
    conn_string = 'postgresql://postgres:admin123@127.0.0.1/postgres'
      
    db = create_engine(conn_string)
    conn = db.connect()

    df2.to_sql('pol_summ', conn, if_exists='append', index=False)

    print('XXX: Inserted summary in postgres ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    return

if __name__ == '__main__':
    print('Pandas is reading mongo and writing the summary to postgres.... ')

    st_time = time.time()
    read_mongo_pd()
    en_time = time.time()

    print('Total time taken was :' + str(en_time-st_time) + ' seconds')
