import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os
import time


def save_to_postgre(st, ed):

    # establish connections
    conn_string = 'postgresql://postgres:admin123@127.0.0.1/postgres'
      
    db = create_engine(conn_string)
    conn = db.connect()

    dfs = []

    print('Preparing DF')
    this_time = time.time()
    for fn in range(st, ed):
        data = pd.read_json('D:\\bigdata\\bigdata-perf\\input\\tr' + str(fn) + '.json', lines=True)
        if data.loc[0, "cancel_date"] == '':
            data.loc[0, "cancel_date"] = '2020-01-01'
        dfs.append(data)  # append the data frame to the list
    print('DF Prepared in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    all_files_df = pd.concat(dfs, ignore_index=True)  # concatenate all the data frames in the list.
    print('Concat in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    all_files_df.to_sql('pol_trans', conn, if_exists='append', index=False)
    print('Inserted in postgre ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    #print(all_files_df)
    return

if __name__ == '__main__':
    print('Enter start of range')
    st = int(input())
    print('Enter count')
    no_of_files = int(input())
    print('Pandas is saving files to postgres.... ')

    st_time = time.time()
    save_to_postgre(st, st + no_of_files)
    en_time = time.time()

    print('Total time taken for ' + str(no_of_files) + ' files was :' + str(en_time-st_time) + ' seconds')
    print('Last file name was ' + str(st + no_of_files - 1))
