import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def read_postgre_pd():
    print('XXX: Creating db connection')
    this_time = time.time()

    # establish connections
    conn_string = 'postgresql://postgres:admin123@127.0.0.1/postgres'
      
    db = create_engine(conn_string)
    conn = db.connect()

    print('XXX: Reading from postgres')
    this_time = time.time()

    df = pd.read_sql('select yyyymm, lob, sum("WP") Written, count(distinct pol_no) totalPolicies, max("WP") MaxWritten, min("WP") MinWritten from pol_trans4 group by yyyymm, lob order by yyyymm, lob', conn);
    
    print('XXX: Pandas DF prepared in ' + str(time.time() - this_time) + ' seconds')
    this_time = time.time()

    df.head()
    
    # print(df1.head())
    print('Total records in df are: ' + str(len(df)))

    # Write to postgres

    #df.to_sql('pol_summ', conn, if_exists='append', index=False)

    #print('XXX: Inserted summary in postgres ' + str(time.time() - this_time) + ' seconds')
    #this_time = time.time()
    
    #conn.close()
    
    return

if __name__ == '__main__':
    print('Pandas is transforming in postgres and writing the summary to postgres.... ')
    st_time = time.time()
    read_postgre_pd()
    en_time = time.time()
    print('Total time taken was :' + str(en_time-st_time) + ' seconds')

