import pymongo
import time
import pandas as pd

def save_to_mongo(st, ed):

    myclient = pymongo.MongoClient()
    db = myclient["local"]
    my_collection = db["p_transactions2"]

    dfs = []  # an empty list to store the data frames

    print('Preparing DF')
    this_time = time.time()
    for fn in range(st, ed):
        data = pd.read_json('D:\\bigdata\\bigdata-perf\\input\\tr' + str(fn) + '.json', lines=True)
        dfs.append(data)  # append the data frame to the list
    print('DF Prepared in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    all_files_df = pd.concat(dfs, ignore_index=True)  # concatenate all the data frames in the list.
    print('Concat in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    all_files_df.reset_index(inplace=True)
    data_dict = all_files_df.to_dict("records")
    print('To dict in ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()
    my_collection.insert_many(data_dict)
    print('Inserted in mongo ' + str(time.time() - this_time) + 'seconds')
    this_time = time.time()

    #print(all_files_df)
    return

if __name__ == '__main__':
    print('Enter start of range')
    st = int(input())
    print('Enter count')
    no_of_files = int(input())
    print('Pandas is saving files to mongodb.... ')

    st_time = time.time()
    save_to_mongo(st, st + no_of_files)
    en_time = time.time()

    print('Total time taken for ' + str(no_of_files) + ' files was :' + str(en_time-st_time) + ' seconds')
    print('Last file name was ' + str(st + no_of_files - 1))
