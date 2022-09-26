#! /usr/bin/env python3
import os, sys
import json
import psycopg2

import paramiko

from scp import SCPClient
from copy import deepcopy
from pytz import timezone
from datetime import datetime, timedelta

from paramiko import SSHClient
from config import user_db, passwd_db, user_linux, passwd_linux


os.chdir(r'/home/james.niu@revintel.net/shadow_claims')


params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db
}
con = psycopg2.connect(**params)


Ymd = datetime.now(tz=timezone('America/New_York')).strftime('%Y%m%d')
md = Ymd[4:] 


cust_l = ('538-734|teamhealth', '631|alteon')    # SPECIFY

for cust in cust_l:
    # SQLS
    sqlgen = """\
select
    created_at
    , content
from
    tpl_pre_billing_records a
where
    cust_id in ({})
    and not exists (select 1 from business_analysis.analyst_shadow_bills b where a.pat_acct = b.pat_acct and a.content->>'vx_carrier_claim_number' = b.vx_carrier_claim_number)
    and created_at::date > '2021-08-01'
	and exists (select 1 from tpl_billing_records c where a.pm_sk = c.pm_sk)
order by
    created_at;"""

    sqlstop = """
    insert into
            business_analysis.analyst_shadow_bills (cust_id, pat_acct, total_charges, vx_carrier_claim_number, created_at)
    values
            ({}, {}, {}, {}, timezone('utc', now()));
    """

#----
# mask json filtering out confidential info
#----
    custid = cust.split('|')[0]
    custno = deepcopy(custid)
    custid = custid.replace('-', ', ')
    custno = custno.replace('-', '_')

    custname = cust.split('|')[1]
    #print(custid, type(custid), custname, type(custname))

    sql = sqlgen.format(custid)
    #print(sql)

    with con:
        cur = con.cursor()
        cur.execute(sql)
    #print('done sql')

    cnt = 0

    with open(f'{custno}_{custname}_prebilling_shadows_{md}.json', 'w') as fw:
        for r in cur:
            dicy = r[1]

            if 'patient_ssn' in dicy:
                del dicy['patient_ssn']

            if 'patient_dob' in dicy:
                del dicy['patient_dob']

            if 'vx_carrier_patient_dob' in dicy:
                del dicy['vx_carrier_patient_dob']

            if 'vx_carrier_patient_birth_year' in dicy:
                del dicy['vx_carrier_patient_birth_year']

            dicy['edi_payer_id'] = '12345'
            dicy['vx_carrier_name'] = 'MEDLYTIX'
            dicy['vx_carrier_address_1'] = '675 MANSELL RD'
            dicy['vx_carrier_address_2'] = ''
            dicy['vx_carrier_city'] = 'ROSWELL'
            dicy['vx_carrier_state'] = 'GA'
            dicy['vx_carrier_zip'] = '30076'
            dicy['vx_carrier_phone'] = '6785070355'

            to_remove = []

            for k0 in dicy.keys():
                if k0[:2] == 'LX' and k0[:4] != 'LX01':
                    to_remove.append(k0)
            for k0 in to_remove:
                del dicy[k0]

            dicy['LX01_charge'] = dicy['total_charges']
            dicy['LX01_procedure_code'] = '99285'

            #print(json.dumps(dicy))
            print(json.dumps(dicy), file=fw)

            cnt += 1
    print('-' * 200)
    print(f'generate {custno}_{custname}_prebilling_shadows_{md}.json | {cnt}')


    #----
    # update database with records already masked
    #----
    with open(f'{custno}_{custname}_update_{md}_shadow.sql', 'w') as fw, open(f'{custno}_{custname}_prebilling_shadows_{md}.json', 'r') as fr:
            for line in fr:
                dicy = json.loads(line)
                line_sql = sqlstop.format(dicy['cust_id'], "'" + dicy['pat_acct'] + "'", dicy['total_charges'], "'" + dicy['vx_carrier_claim_number'] + "'")

                print(line_sql.strip(), file=fw)

    cnt = 0

    with open(f'{custno}_{custname}_update_{md}_shadow.sql', 'r') as fr:
        tmp = fr.read()
        sql_list = tmp.split(';')[:-1]

        for sql in sql_list:
            with con:
                cur = con.cursor()
                cur.execute(sql)

                cnt += 1
                    
    print(f'{custno}_{custname}_update_{md}_shadow.sql | {cnt}')


    #----
    # push to linux server folder /tmp
    #----
    if cnt != 0:
        try:
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.load_system_host_keys()
            ssh.connect(hostname='revproc01.revintel.net',
                        username=user_linux,
                        password=passwd_linux)

            # SCPCLient takes a paramiko transport as its only argument
            scp = SCPClient(ssh.get_transport())
            scp.put(f'/home/james.niu@revintel.net/shadow_claims/{custno}_{custname}_prebilling_shadows_{md}.json', '/tmp')

            print('\nscp transport success...')
        except:
            print('\nconnection ERROR')
    else:
        print('\nno new file...')
print('-' * 200)

