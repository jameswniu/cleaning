#! /usr/bin/python3
import sys
import json
import re
import os

from datetime import datetime as dt


jar = {}

with open('template.txt', 'r') as fh:
    for line in fh:

        if not re.match(r'^\d{7}\s[|]\s', line):
            continue

        pm_sk = re.search(r'\d{7}', line).group()
        new_name = re.search(r'(?<=changed to\s)(.*)(?=[)])', line).group() 
        
        if '[' not in new_name: 
            jar[pm_sk] = [new_name, line.strip()]

    #print(jar, file=sys.stderr)


kk = {}
mm = {}

with open('template.txt', 'r') as fh:
    for line in fh:
        if not re.match(r'^I\d{7}[|]', line):
            continue

        pm_sk = re.search(r'\d{7}', line).group()
        kk[pm_sk] = line.strip()
        
        zp = re.search(r'(?<=to add zip ).*?(?=[,)])', line).group()

        try:
            addr1 = re.search(r'(?<=to change addr1 ).*?(?=[,)])', line).group().replace('to', '').strip()
        except:
            addr1 = ''
        try:
            ci = re.search(r'(?<=to change city ).*?(?=[,)])', line).group().replace('to', '').strip()
        except:
            ci = ''
        try:
            st = re.search(r'(?<=to change state ).*?(?=[,)])', line).group().replace('to', '').strip()
        except:
            st = ''
        mm[pm_sk] = '{}--{}--{}--{}'.format(zp, addr1, ci, st)
#print(kk, file=sys.stderr)
#print(mm, file=sys.stderr)


c, d = 0, 0

for line in sys.stdin:
    dicy = json.loads(line)
    
    if not re.match(r'\s{0,6}{', line):
        continue

    if dicy['vx_pm_sk'] in jar:    # trim last name
        if dicy['vx_carrier_insured_last_name'] != jar[dicy['vx_pm_sk']][0]: 
            dicy['vx_carrier_insured_last_name'] = jar[dicy['vx_pm_sk']][0]        

            print(jar[dicy['vx_pm_sk']][1], file=sys.stderr)          
            
            c += 1    

    if dicy['vx_pm_sk'] in kk:    # remove missing zip
        print(kk[dicy['vx_pm_sk']], file=sys.stderr)         
        
        d += 1
        continue
        
    r = json.dumps(dicy)
    print(r)
print("""insured lastname changed {}""".format(c), file=sys.stderr)
print("""insured zip removed {}""".format(d), file=sys.stderr)


md = dt.now().strftime('%m%d')
processed = {}

if d != 0:
    if os.path.exists('/tmp/update_{}_zip.sql'.format(md)):
        with open('/tmp/update_{}_zip.sql'.format(md), 'r') as fr:
            for line in fr:
                processed[line.strip()] = 1

        append_write = 'a'
    else:
        append_write = 'w'


    sql_file = open('/tmp/update_{}_zip.sql'.format(md), append_write)

    str = """update tpl_pre_billing_records set content = content || '{{{}}}' where pm_sk = {};"""

    for k in mm:
        demo = mm[k].split('--')
        
        info = '"vx_carrier_insured_zip": "{}"'.format(demo[0])

        if demo[1] != '':
            info = info + ', ' + '"vx_carrier_insured_address_1": "{}"'.format(demo[1])
        if demo[2] != '':
            info = info + ', ' + '"vx_carrier_insured_city": "{}"'.format(demo[2])
        if demo[3] != '':
            info = info + ', ' + '"vx_carrier_insured_state": "{}"'.format(demo[3])

        # print(info)
        ss = str.format(info, k)

        if ss not in processed:
            print(ss, file=sql_file)

    sql_file.close()

