import pymongo
import json
import time


def save_to_mongo(fname):
    myclient = pymongo.MongoClient()
    db = myclient["local"]
    my_collection = db["p_transactions"]
    with open('D:\\bigdata\\bigdata-perf\\input\\tr' + str(fname) + '.json') as file:
        file_data = json.load(file)
    my_collection.insert_one(file_data)
    return

if __name__ == '__main__':
    print('Enter start of range')
    st = int(input())
    print('Enter count')
    no_of_files = int(input())
    print('Saving files to mongodb.... ')

    st_time = time.time()
    for fn in range(st, st+no_of_files):
        save_to_mongo(fn)
    en_time = time.time()

    print('Total time taken for ' + str(no_of_files) + ' files was :' + str(en_time-st_time) + ' seconds')
    print('Last file name was ' + str(st + no_of_files - 1))
