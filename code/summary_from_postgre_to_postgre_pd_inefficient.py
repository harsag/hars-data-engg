import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def read_postgre_pd_o():
    print('XXX: Creating db connection')
    this_time = time.time()

    # establish connections
    conn_string = 'postgresql://postgres:admin123@127.0.0.1/postgres'
      
    db = create_engine(conn_string)
    conn = db.connect()

    print('XXX: Reading from postgres')
    this_time = time.time()

    df = pd.read_sql('select yyyymm, lob, "WP", pol_no from pol_trans4', conn);
    
    print('XXX: Pandas DF prepared in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    # print(df1.head())
    print('Total records in df are: ' + str(len(df)))

    # Transform
    df2 = df.groupby(['yyyymm','lob']).agg({'WP': ['sum', 'max', 'min'], 'pol_no': 'nunique'})
    df2.columns = ['Written', 'MaxWritten', 'MinWritten', 'totalPolicies']
    df2.reset_index(inplace=True)

    print('XXX: Pandas DF transformed in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    df2.head()

    print('Total records in df2 are: ' + str(len(df2)))

    # Write to postgres

    #df2.to_sql('pol_summ', conn, if_exists='append', index=False)

    #print('XXX: Inserted summary in postgres ' + str(time.time() - this_time) + ' seconds')
    #this_time = time.time()
    
    conn.close()
    
    return

if __name__ == '__main__':
    print('Pandas is reading postgres, transoforming in pandas and writing the summary to postgres.... ')
    st_time = time.time()
    read_postgre_pd_o()
    en_time = time.time()
    print('Total time taken was :' + str(en_time-st_time) + ' seconds')
