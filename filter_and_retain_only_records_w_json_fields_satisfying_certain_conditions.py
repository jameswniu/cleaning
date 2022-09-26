import os
import sys
import json
import re


os.chdir('L:\\Auto_Opportunity_Analysis\\Json_Samples')


# ------------------------------------------
# check vnex dol greater than lx01 dos
# ------------------------------------------
cnt = 0
tot = 1

l = []

with open('0308_edi.json', 'r') as fr:
    for line in fr:

        if not re.match(r'\s{0,6}{', line):
            continue

        dicy = json.loads(line)
        tot += 1

        try:
            service_date = 'cust dos ' + dicy['service_date']
        except:
            service_date = ''

        if dicy['vx_date_of_loss'] <= dicy['LX01_service_date']:
            continue

        l.append('{}|{}|vx dol {} -> {} LX01 dos|{}'.format(dicy['cust_id'], dicy['pat_acct'], dicy['vx_date_of_loss'], dicy['LX01_service_date'], service_date))
        cnt += 1

[print(rec) for rec in sorted(l)]
print('{} / {}'.format(cnt, tot))


# ------------------------------------------
# generate new json
# ------------------------------------------
tot = 0

with open('0308_edi.json', 'r') as fr, open('0308_edi1.json', 'w') as fw0:
    for line in fr:

        if not re.match(r'\s{0,6}{', line):
            continue

        dicy = json.loads(line)

        if dicy['vx_date_of_loss'] <= dicy['LX01_service_date']:
            print(line, end='', file=fw0)
            tot += 1

print(tot)















# with open('neww_2.txt', 'w') as fw0:
#     with open('new_2.txt', 'r') as fr:
#         for line in fr:
#             tmp = line.strip().split(' ')
#             tmp1 = line.strip().split(' - ')
#             code = '277'+'-'+tmp[0].split('-')[0].replace("'", "")
#             # print(code)
#             # print(code + ', ' + tmp1[0].strip() + ' - ' + tmp1[1].strip())
#             new_line = code + ', ' + tmp1[0].strip().replace('-', ':') + ' - ' + tmp1[1].strip()
#             fw0.write(new_line  + '\n')

# with open('neww_1.txt', 'w') as fw0:
#     with open('new_1.txt', 'r') as fr:
#         for line in fr:
#             tmp = line.split('\t')
#             new_line = tmp[0] + ', ' + tmp[1].replace('\n', '')
#             print(new_line)
#             fw0.write(new_line + '\n')

# all_code = set()
# with open('neww_1.txt', 'r') as fr:
#     for line in fr:
#         all_code.add(line)
# with open('neww_2.txt', 'r') as fr:
#     for line in fr:
#         all_code.add(line)
# sorted_code = sorted(list(all_code), key=lambda x: x[1])
# with open('neww_final.txt', 'w') as fw0:
#     [fw0.write(line) for line in sorted_code]


# with open('jopari_errors_list_20200205.txt', 'r') as fr:
#     for line in fr:
#         tmp = line.split(',')
#         new_line = ", ('" + tmp[0] + "', " + "'" + tmp[1].strip() + "')"
#         fw0.write(new_line)