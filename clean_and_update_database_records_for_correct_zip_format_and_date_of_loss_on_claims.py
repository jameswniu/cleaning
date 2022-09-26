#! /usr/bin/python3
import os, sys
import re
import json

from datetime import datetime as dt


sss0 = {}
sss1 = {}  
qqq0 = {}
c, d, g = 0, 0, 0
 
for line in sys.stdin:
    if not re.match(r'\s{0,6}{', line):
        continue

    line = line.replace("'", "")    # JSON should not have single quotes    
    
    d += 1 
    dicy = json.loads(line)
    
    try:
        dicy['send_fax_number'] = ''.join(re.findall(r'\d', dicy['send_fax_number']))    # fax nos should not have hypen
    except:
        pass
    
    try:
        dicy['dx_code_desc'] = dicy['dx_code_desc'].upper().strip()    # dx code desc all upper case and strip spaces
    except:
        pass


#----
# update facility or pay to zip and generate sql
#----
    try: 
        for prefix in ('pay_to', 'facility'):
            if dicy['{}_zip'.format(prefix)] == '44710' and dicy['{}_addr1'.format(prefix)] == '2600 SIXTH ST SW' and dicy['{}_city'.format(prefix)] == 'CANTON' and dicy['{}_state'.format(prefix)] == 'OH':
                dicy['{}_zip'.format(prefix)] = '447101702'
                q0 = """update tpl_pre_billing_records set content = content || '{{"{}_zip": "447101702"}}' where pm_sk = {};"""
                qq0 = q0.format(prefix, dicy['vx_pm_sk'])
                qqq0[qq0] = 1

                g += 1

                #print(qq0, file=sys.stderr)
                print('{}  {} 44710 >> 447101702'.format(dicy['vx_pm_sk'], '{}_zip'.format(prefix)), file=sys.stderr) 
    except:
        pass

#----
# remove dol greater than service date and generate sql
#----
    if dicy['claim_type'] == '837P': 
        if dicy['vx_date_of_loss'] > dicy['LX01_service_date']:
            if (dt.strptime(dicy['vx_date_of_loss'], '%Y%m%d') - dt.strptime(dicy['LX01_service_date'], '%Y%m%d')).days > 7:
                s0 = """update tpl_pre_billing_records set status = 'E', note = 'accident date is much later than service date' where pm_sk = {};""" 
                ss0 = s0.format(dicy['vx_pm_sk'])
                sss0[ss0] = 1            
                c += 1
            else:
                s1 = """update tpl_pre_billing_records set content = content || jsonb_build_object('vx_date_of_loss', content->>'LX01_service_date') where pm_sk = {};"""
                ss1 = s1.format(dicy['vx_pm_sk'])
                sss1[ss1] = 1            
                c += 1 
        else:
            print(json.dumps(dicy))

    else: 
        print(f"837I detected: {dicy['vx_pm_sk']}|{dicy['cust_id']}|{dicy['pat_acct']}", file=sys.stderr)

        if dicy['vx_date_of_loss'] > dicy['LX001_service_date']:
            if (dt.strptime(dicy['vx_date_of_loss'], '%Y%m%d') - dt.strptime(dicy['LX001_service_date'], '%Y%m%d')).days > 7:
                s0 = """update tpl_pre_billing_records set status = 'E', note = 'accident date is much later than service date' where pm_sk = {};""" 
                ss0 = s0.format(dicy['vx_pm_sk'])
                sss0[ss0] = 1            
                c += 1
            else:
                s1 = """update tpl_pre_billing_records set content = content || jsonb_build_object('vx_date_of_loss', content->>'LX001_service_date') where pm_sk = {};"""
                ss1 = s1.format(dicy['vx_pm_sk'])
                sss1[ss1] = 1            
                c += 1 
        else:
            print(json.dumps(dicy))
print('{}/{}'.format(c, d), file=sys.stderr)


md = dt.now().strftime('%m%d')
processed = {}
processed1 = {}


#----
# create or append to facility/pay to sql file 
#----
if g != 0:
    if os.path.exists('/tmp/facilitypayto_zip_{}.sql'.format(md)):
        with open('/tmp/facilitypayto_zip_{}.sql'.format(md), 'r') as fr:
            for line in fr:
                processed1[line.strip()] = 1
     
        append_write = 'a'
    else:
        append_write = 'w'

    
    sql_file = open('/tmp/facilitypayto_zip_{}.sql'.format(md), append_write)

    for qq0 in qqq0:
        if qq0 not in processed1:
            print(qq0, file=sql_file)

    sql_file.close()


#----
# create or append to update dol sql file  
#----
if c != 0:
    if os.path.exists('/tmp/update_{}_accident.sql'.format(md)):
        with open('/tmp/update_{}_accident.sql'.format(md), 'r') as fr:
            for line in fr:
                processed[line.strip()] = 1
     
        append_write = 'a'
    else:
        append_write = 'w'

    
    sql_file = open('/tmp/update_{}_accident.sql'.format(md), append_write)

    for ss0 in sss0:
        if ss0 not in processed:
            print(ss0, file=sql_file)

    for ss1 in sss1:
        if ss1 not in processed: 
            print(ss1, file=sql_file)

    sql_file.close()



