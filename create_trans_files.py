import json
import random
import datetime
import time


def get_cancel_date(sdate):
    rdays = random.choice(list(range(1,200)))
    ch = random.choice([0,1,2,3])
    ldate = datetime.datetime.strptime(sdate, '%Y-%m-%d')
    if ch == 0:
        return (ldate + datetime.timedelta(rdays)).strftime('%Y-%m-%d')
    else:
        return ''


def get_random_date():
    day = list(range(1,28))
    mon = list(range(1,13))
    yr = ['2022','2021','2020']

    my_sdt = datetime.datetime(int(random.choice(yr)), random.choice(mon), random.choice(day))
    my_edt = my_sdt + datetime.timedelta(days=365)
    sdt = my_sdt.strftime('%Y-%m-%d')
    edt = my_sdt.strftime('%Y-%m-%d')

    return sdt,edt

def create_trans(tr_no):
    # Domain
    file_name = 'D:\\bigdata\\bigdata-perf\\input\\tr' + str(tr_no) + '.json'
    begin_dt, end_dt  = get_random_date()
    cancel_dt = get_cancel_date(begin_dt)
    wp = random.choice(list(range(400,2000)))
    st = random.choice(['CA','NV','AZ','OR'])
    terrs = {'CA' : ['11','12','13','14','16'],
             'NV': ['22','26','28','29','24','27'],
             'AZ': ['32','36','38','39','34','37'],
             'OR': ['42','46','48','49','44','47']
             }
    terr = random.choice(terrs[st])
    pr = random.choice(['P1','P2','P3','P4'])
    lob = random.choice(['Auto','Home','Boat'])

    # print('Random begin date is ' + str(begin_dt) )
    # print('Random end date is ' + str(end_dt) )
    # print('Random cancel date is ' + str(cancel_dt) )
    # print('Other is ' + str(wp) + ' ' + st + ' ' + terr + ' ' + pr + ' ' + lob)

    # Add data
    my_data = {}
    my_data['trans_no'] = 'TR' + str(tr_no)
    my_data['pol_no'] = 'TR-' + str(tr_no)
    my_data['begin_date'] = begin_dt
    my_data['end_date'] = end_dt
    my_data['inception_date'] = begin_dt
    my_data['cancel_date'] = cancel_dt
    my_data['WP'] = wp
    my_data['state'] = st
    my_data['terr'] = terr
    my_data['program'] = pr
    my_data['lob'] = lob

    my_json = json.dumps(my_data)
    # print(my_json)

    # Write file
    with open(file_name, 'w') as outfile:
        outfile.write(my_json)

    # print('Hello')

def create_files(st, no_of_files):
    st_time = time.time()
    for fn in range(st, st+no_of_files):
        create_trans(fn)
    en_time = time.time()

    print('Total time taken for ' + str(no_of_files) + ' files was :' + str(en_time-st_time) + ' seconds')
    print('Last file name was ' + str(st + no_of_files - 1))

if __name__ == '__main__':
    print('Enter start of range')
    st = int(input())
    print('Enter count')
    no_of_files = int(input())
    create_files(st, no_of_files)