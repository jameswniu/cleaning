#! /usr/bin/env python3
import os
import sys
import re
import time
import json

import collections
import psycopg2

from datetime import datetime
from string import punctuation as pun

from config import user_db, passwd_db


params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db}
con = psycopg2.connect(**params)


ST_MAP = {
    "ARMED FORCES AFRICA": "AE",
    "ARMED FORCES AMERICAS (EXCEPT CANADA)": "AA",
    "ARMED FORCES CANADA": "AE",
    "ARMED FORCES EUROPE": "AE",
    "ARMED FORCES MIDDLE EAST": "AE",
    "ARMED FORCES PACIFIC": "AP",
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "ARMED FORCES PACIFIC": "AP", 
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "VIRGIN ISLANDS": "VI",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY"
}


def levenshtein(s, t):
        ''' From Wikipedia article; Iterative with two matrix rows. '''
        if s == t: return 0
        elif len(s) == 0: return len(t)
        elif len(t) == 0: return len(s)
        v0 = [None] * (len(t) + 1)
        v1 = [None] * (len(t) + 1)
        for i in range(len(v0)):
            v0[i] = i
        for i in range(len(s)):
            v1[0] = i + 1
            for j in range(len(t)):
                cost = 0 if s[i] == t[j] else 1
                v1[j + 1] = min(v1[j] + 1, v0[j + 1] + 1, v0[j] + cost)
            for j in range(len(v0)):
                v0[j] = v1[j]

        return v1[len(t)]


def double_barrel(X, Y):

    m = len(X)
    n = len(Y)

    LCSuff = [[0 for k in range(n+1)] for l in range(m+1)]

    result = 0

    # Following steps to build
    # LCSuff[m+1][n+1] in bottom up fashion
    for i in range(m + 1):
        for j in range(n + 1):
            if (i == 0 or j == 0):
                LCSuff[i][j] = 0
            elif (X[i-1] == Y[j-1]):
                LCSuff[i][j] = LCSuff[i-1][j-1] + 1
                result = max(result, LCSuff[i][j])
            else:
                LCSuff[i][j] = 0

    return result == min(m, n)



def check_claim_party(res):
    if res['vx_carrier_insured_last_name'] == res['patient_lastname'] and res['vx_carrier_insured_first_name'] == res['patient_firstname']:
        if 'PERSON' not in res['vx_carrier_lob'] and 'COMMERC' not in res['vx_carrier_lob']:
            print('{}  {}|{}  {}|{}  should be FP({}) {}'.format(x,  res['vx_carrier_insured_first_name'], res['patient_firstname'],
                res['vx_carrier_insured_last_name'], res['patient_lastname'], res['vx_claim_type'], res['vx_carrier_lob']
            ))

        if res['vx_claim_type'] != 'FP':
            if 'patient_phone' in res: 
                print('{} {} : {}|{}  --  {}|{} -- {}|{} | {} -- {}|{} | {}  - {}'.format(x, res['vx_claim_type'],
                      res['vx_carrier_insured_first_name'],     res['patient_firstname'],
                      res['vx_carrier_insured_last_name'],      res['patient_lastname'],
                      res['vx_carrier_insured_address_1'],      res['vx_carrier_patient_address_1'],      res['patient_addr1'],
                      res['vx_carrier_insured_phone'],          res['vx_carrier_patient_phone'],          res['patient_phone'], 
                      res['vx_carrier_name']))
            else:
                print('{} {} : {}|{}  --  {}|{} -- {}|{} | {} -- {}|{} | {}  - {}'.format(x, res['vx_claim_type'],
                      res['vx_carrier_insured_first_name'],     res['patient_firstname'],
                      res['vx_carrier_insured_last_name'],      res['patient_lastname'],
                      res['vx_carrier_insured_address_1'],      res['vx_carrier_patient_address_1'],      res['patient_addr1'],
                      res['vx_carrier_insured_phone'],          res['vx_carrier_patient_phone'],          '',
                      res['vx_carrier_name']))

    elif levenshtein( res['vx_carrier_insured_first_name'], res['patient_firstname'] ) < 2   or \
           levenshtein( res['vx_carrier_insured_last_name'], res['patient_lastname'] ) < 2   :
        if res['vx_claim_type'] == 'FP':
            print('{} {} : {}|{}  --  {}|{}'.format(x, res['vx_claim_type'], res['vx_carrier_insured_first_name'],
                        res['patient_firstname'], res['vx_carrier_insured_last_name'], res['patient_lastname']))
        elif res['vx_claim_type'] == 'TP':
            if 'patient_phone' in res: 
                print('{} {} : {}|{}  --  {}|{} -- {}|{} | {} -- {}|{} | {}  - {}'.format(x, res['vx_claim_type'],
                      res['vx_carrier_insured_first_name'],     res['patient_firstname'],
                      res['vx_carrier_insured_last_name'],      res['patient_lastname'],
                      res['vx_carrier_insured_address_1'],      res['vx_carrier_patient_address_1'],      res['patient_addr1'],
                      res['vx_carrier_insured_phone'],          res['vx_carrier_patient_phone'],          res['patient_phone'], 
                      res['vx_carrier_name']))
            else:
                print('{} {} : {}|{}  --  {}|{} -- {}|{} | {} -- {}|{} | {}  - {}'.format(x, res['vx_claim_type'],
                      res['vx_carrier_insured_first_name'],     res['patient_firstname'],
                      res['vx_carrier_insured_last_name'],      res['patient_lastname'],
                      res['vx_carrier_insured_address_1'],      res['vx_carrier_patient_address_1'],      res['patient_addr1'],
                      res['vx_carrier_insured_phone'],          res['vx_carrier_patient_phone'],          '',
                      res['vx_carrier_name']))

    elif double_barrel(res['vx_carrier_insured_last_name'], res['patient_lastname']) :
        if res['vx_claim_type'] == 'TP':
            print_fmt = '{} {} : {}|{}  --  {}|{} -- {}|{} | {} -- {}|{} | {}  - {}'
            if 'patient_phone' in res:
                print((print_fmt).format(x, res['vx_claim_type'],
                            res['vx_carrier_insured_first_name'], res['patient_firstname'],
                            res['vx_carrier_insured_last_name'],  res['patient_lastname'],
                            res['vx_carrier_insured_address_1'],  res['vx_carrier_patient_address_1'],  res['patient_addr1'],
                            res['vx_carrier_insured_phone'],      res['vx_carrier_patient_phone'],      res['patient_phone'],
                            res['vx_carrier_name']))
            else:
                print((print_fmt).format(x, res['vx_claim_type'],
                            res['vx_carrier_insured_first_name'], res['patient_firstname'],
                            res['vx_carrier_insured_last_name'],  res['patient_lastname'],
                            res['vx_carrier_insured_address_1'],  res['vx_carrier_patient_address_1'],  res['patient_addr1'],
                            res['vx_carrier_insured_phone'],      res['vx_carrier_patient_phone'],      '',
                            res['vx_carrier_name']))



def print_no_zip_address(res):
    if not res['vx_carrier_insured_zip'] :
        print('I{}|{},{},{} {}'.format(x,  res['vx_carrier_insured_address_1'], res['vx_carrier_insured_city'],
                  res['vx_carrier_insured_state'], res['vx_carrier_insured_zip']))

    if not res['patient_zip']:
        print('P{}|{},{},{} {}'.format(x, res['patient_addr1'], res['patient_city'],
              res['patient_state'], res['patient_zip'] ))
    
    if res['vx_carrier_insured_address_1'] in ST_MAP:
        print('{}|{}|vx_carrier_insured_address_1: {}'.format(x, res['vx_carrier_insured_last_name'], res['vx_carrier_insured_address_1'])) 


def check_state_zip_length(res):
    if res['claim_type'] == '837I':
        state_fields = [
            "pay_to_state", "patient_state",
            "billing_provider_state" ,
            "vx_carrier_state", "vx_accident_state",
            "vx_carrier_insured_state", "vx_carrier_patient_state" ]

        zip_fields = [
            "pay_to_zip", "patient_zip",
            "billing_provider_zip",
            "vx_carrier_insured_zip",
            "vx_carrier_patient_zip" ]
    else:
        state_fields = [
            "pay_to_state", "patient_state",
            "facility_state",
            "billing_provider_state",
            "vx_carrier_state" , "vx_accident_state",
            "vx_carrier_insured_state",
            "vx_carrier_patient_state" ]
        zip_fields = [
            "pay_to_zip", "patient_zip",
            "facility_zip", "billing_provider_zip",
            "vx_carrier_insured_zip",
            "vx_carrier_patient_zip" ]


    msg = ""
    for azip in zip_fields:
        xzip = re.sub(r'\D', '', res[azip])
        if 'vx_carrier_p' in azip: 
            if len(xzip) not in [5, 9]:
                msg += """bad "{}": {}, """.format(azip, res[azip])

        else: 
            if len(xzip) not in [5, 9]:
                msg += """invalid "{}": {}, """.format(azip, res[azip])

            for kw in ['facility', 'pay_to_zip']:
                if kw in azip and len(xzip) != 9:
                    msg += """invalid "{}": {}, """.format(azip, res[azip])

    state_list = ST_MAP.values()
    for astate in state_fields:
        xst = re.sub(r'\s', '', res[astate])
        if 'vx_carrier_p' in astate: 
            if xst not in state_list:
                msg += """bad "{}": {}, """.format(astate, res[astate])
        
        else: 
            if xst not in state_list:
                msg += """invalid "{}": {}, """.format(astate, res[astate])

    if msg:
        print("{}: {}".format(x, msg))


def print_commercial_insurance_holder(res):
    if not res['vx_carrier_insured_first_name'] and len(res['vx_carrier_insured_last_name']) > 30:
        print('{} | {}'.format(x, res['vx_carrier_insured_last_name']))


def check_insured_fullname(res):
    if not re.search(r'COMMERC|WORKER|EMPLOYE|STATE', res['vx_carrier_lob'], re.I):
        if '&' in res['vx_carrier_insured_first_name']:
            if (res['patient_firstname'] in res['vx_carrier_insured_first_name']
                    and (res['patient_lastname'] in res['vx_carrier_insured_last_name'] or res['vx_carrier_insured_last_name'] in res['patient_lastname'])):
                print('{} : invalid insured first name - {} - {} (changed to {})'.format(x, res['vx_carrier_lob'], res['vx_carrier_insured_first_name'], res['patient_firstname']), file=sys.stderr)

                res['vx_carrier_insured_first_name'] = res['patient_firstname']

            elif (res['vx_carrier_patient_first_name'] in res['vx_carrier_insured_first_name'] 
                    and (res['vx_carrier_patient_last_name'] in res['vx_carrier_insured_last_name'] or res['vx_carrier_insured_last_name'] in res['vx_carrier_patient_last_name'])):
                print('{} : invalid insured first name - {} - {} (changed to {})'.format(x, res['vx_carrier_lob'], res['vx_carrier_insured_first_name'], res['vx_carrier_patient_first_name']), file=sys.stderr)

                res['vx_carrier_insured_first_name'] = res['vx_carrier_patient_first_name']

            else:
                newfirstname = ' '.join([word for word in re.sub(r'\s+', ' ', res['vx_carrier_insured_first_name']).split(' ') if not(len(word) == 1 and word != '&')])
                
                mkfirstname = re.search(r'^.*?(?=\s|\s&)', newfirstname).group()

                print('{} : invalid insured first name - {} - {} (changed to [{}])'.format(x, res['vx_carrier_lob'], res['vx_carrier_insured_first_name'], mkfirstname), file=sys.stderr)

                res['vx_carrier_insured_first_name'] = mkfirstname

        if '&' in res['vx_carrier_insured_last_name']:
            if (res['patient_lastname'] in res['vx_carrier_insured_last_name'] 
                    and (res['patient_firstname'] in res['vx_carrier_insured_first_name'] or res['vx_carrier_insured_first_name'] in res['patient_firstname'])):
                print('{} : invalid insured last name - {} - {} (changed to {})'.format(x, res['vx_carrier_lob'], res['vx_carrier_insured_last_name'], res['patient_lastname']), file=sys.stderr)

                res['vx_carrier_insured_last_name'] = res['patient_lastname']

            elif (res['vx_carrier_patient_last_name'] in res['vx_carrier_insured_last_name'] 
                    and (res['vx_carrier_patient_first_name'] in res['vx_carrier_insured_first_name'] or res['vx_carrier_insured_first_name'] in res['vx_carrier_patient_first_name'])):
                print('{} : invalid insured last name - {} - {} (changed to {})'.format(x, res['vx_carrier_lob'], res['vx_carrier_insured_first_name'], res['vx_carrier_patient_first_name']), file=sys.stderr)

                res['vx_carrier_insured_last_name'] = res['vx_carrier_patient_last_name']

            else:
                newlastname = ' '.join([word for word in re.sub(r'\s+', ' ', res['vx_carrier_insured_last_name']).split(' ') if not(len(word) == 1 and word != '&')])
                
                mklastname = re.search(r'^.*?(?=\s|\s&)', newlastname).group()

                print('{} : invalid insured last name - {} - {} (changed to [{}])'.format(x, res['vx_carrier_lob'], res['vx_carrier_insured_last_name'], mklastname), file=sys.stderr)

                res['vx_carrier_insured_last_name'] = mklastname


custname = {
    "321": "ApolloMD Athena",
    "405": "ApolloMD Athena",
    "67": "ApolloMD IDX",
    "484": "AULTMAN HOSPITAL",
    "497": "Logix Health",
    "530": "Western Medical Associates",
    "565": "EMA",
    "564": "VEP",
    "538": "Team Health",
    "171": "Texas Health Resources",
    "483": "Texas Health Resources",
    "366": "Beaumont Health",
    "18": "Adventist System",
    "227": "Johns Hopkins",
    "646": "AULTMAN PROFESSIONAL",
    "678": "Western Medical Associates",
    "631": "Alteon Health",
    "710": "TeamHealth Work Comp", 
    "734": "Team Health AutoPR"
}
def check_lockbox(res):
    if res['cust_id'] in ('321', '405'):
        if '{},{},{} {}'.format(res['pay_to_addr1'], res['pay_to_city'], res['pay_to_state'], res['pay_to_zip']) == 'PO BOX 740937,ATLANTA,GA 303740937':
            ind = '| y'        
        else:
            ind = '| n'

    elif res['cust_id'] in ('538', '734'):
        if '{},{},{} {}'.format(res['pay_to_addr1'], res['pay_to_city'], res['pay_to_state'], res['pay_to_zip']) == 'PO BOX 740011,ATLANTA,GA 303740011':
            ind = '| y'        
        else:
            ind = '| n'

    elif res['cust_id'] == '631':
        if '{},{},{} {}'.format(res['pay_to_addr1'], res['pay_to_city'], res['pay_to_state'], res['pay_to_zip']) == 'PO BOX 746744,ATLANTA,GA 303746744':
            ind = '| y'        
        else:
            ind = '| n'    
    
    elif res['pay_to_addr1'].strip() == '' or res['pay_to_city'].strip() == '' or res['pay_to_state'].strip() == '' or res['pay_to_zip'].strip() == '':
            ind = '| n'

    else:
        ind = '' 

    print(' {} {} | {},{},{} {} {}'.format(res['cust_id'], custname[res['cust_id']].upper() 
                                        ,  res['pay_to_addr1'], res['pay_to_city']
                                        ,  res['pay_to_state'], res['pay_to_zip'], ind))


def check_balance(res):
    if abs(float(res["balance"]) - float( res["total_charges"])) > 0.02 :
        print('{}|charges {} - {} bal'.format(x,  res["total_charges"] , res["balance"] ))

def check_fax(res):
    if len(re.findall(r'\d', res["send_fax_number"])) == 10:
        ind = "y"
    else:
        ind = "n"
    
    print(' {} | {} | {}'.format( res["vx_carrier_name"], res["send_fax_number"], ind))
     
    if not res["send_fax_number"]:
        print('{}|{}'.format(res["vx_pm_sk"], res["vx_carrier_name"]))    


def check_quotes_clean(res):
    msg = ''
    cup = {}
    for k, v in res.items():
        if isinstance(v, str) and "'" in v:
            msg = v
            new_val = re.sub(r'[\']+', "'", v)
            res[k] = new_val

    #----
    # clean duplicate svc line modifier
    #----
        if 'LX' in k and 'modifier' in k:
            if v not in cup.values():
                cup[k] = v
            else:
                cup[k] = ''

            res[k] = ''

        for k in res:
            if k in cup:
                res[k] = cup[k]

    #----
    # if address state is long, use abbreviation
    #----
    for ky in res:
        for name in ST_MAP:
            if 'state' in ky:
                if res[ky] == name:
                    res[ky] = ST_MAP[name] 

    #----
    # if vx accident state state is wrong, use cust accident state
    #----
    for ky in res:
        if ky == 'vx_accident_state':
            if res[ky] not in ST_MAP.values():
                res[ky] = res['accident_state'] 

    #----
    # clean punctuation
    #----
    for item in [
                    'billing_provider', 'billing_provider_addr1',
                    'vx_carrier_address_1', 'vx_carrier_address_2', 
                    'vx_carrier_insured_address_1', 'vx_carrier_patient_address_1',
                    'patient_addr1'
                ]:
        
        res[item] = re.sub(r'\s+', ' ', res[item]).strip()

        for q in (',', '.'):
            if q in res[item]:
                res[item] = re.sub(rf'\s*[{q}]+\s*|\s+', ' ', res[item]).strip()

        for r in ('/'):
            if r in res[item]:
                res[item] = re.sub(rf'\s*[{r}]+\s*', '', res[item]).strip()

    for item in [
                    'patient_firstname', 'vx_carrier_insured_first_name',
                    'patient_lastname', 'vx_carrier_insured_last_name'
                ]: 

        res[item] = re.sub(r'\s+', ' ', res[item]).strip()

        for p in pun.replace('&', ''):
            if p in res[item]:
                res[item] = re.sub(r'\s*{}+\s*|\s+'.format(f'\{p}'), ' ', res[item]).strip()

        res[item] = re.sub(r'\bESTATE\s*OF\b|\bTHE\s*ESTATE\s*OF\b', '', res[item]).strip()
        res[item] = re.sub(r'\s+', ' ', res[item]).strip()

    #----
    # if patient address is PO BOX, use Venx patient address
    #----
    pat_street = res['patient_addr1'].replace(' ', '').replace('.', '').upper()

    if 'POBOX' in pat_street:
        res['patient_addr1'] = res['vx_carrier_patient_address_1']
        res['patient_addr2'] = ''
        res['patient_city']  = res['vx_carrier_patient_city']
        res['patient_state'] = res['vx_carrier_patient_state']
        if res['vx_carrier_patient_zip'] != '':
            res['patient_zip']   = res['vx_carrier_patient_zip']
    
    if re.match(r'\d+$', res['vx_carrier_insured_zip']): 
        res['vx_carrier_insured_zip'] = res['vx_carrier_insured_zip'][:5]   # GEICO only takes zip5

    if re.match(r'\d+$', res['patient_zip']): 
        res['patient_zip']  = res['patient_zip'][:5]

    return msg


def correct_common_errors(res):
    #----
    # correct lob
    #----
    if "MICHIGAN ASSIGNED CLAIMS PLAN" in [res["vx_carrier_name"], res["vx_carrier_insured_last_name"]]:
        res["vx_carrier_lob"] = "AUTO (STATE)"

    if res['vx_carrier_insured_last_name'] in ('MVAIC', 'NJPLIGA'):    # SPECIFY
        res['vx_carrier_lob'] = 'AUTO (COMMERCIAL)'

    if res['vx_carrier_insured_last_name'] == 'MVAIC':
        res['vx_carrier_insured_address_1'] = '100 WILLIAM ST'

    #----
    # correct edi payer id
    #----
    if "edi_payer_id" in res:
        if 'TRAVELERS' in res['vx_carrier_name'] and "WORKERS COMP" in res['vx_carrier_lob']:
            res['edi_payer_id'] = '19046'

    if "edi_payer_id" in res:
        if 'CHUBB' in res['vx_carrier_name'] and "WORKERS COMP" in res['vx_carrier_lob']:
            res['edi_payer_id'] = 'J1554'

    if "edi_payer_id" in res:
        if 'STATE FARM' in res['vx_carrier_name'] and "WORKERS COMP" in res['vx_carrier_lob']:
            res['edi_payer_id'] = 'J3349'

    if "edi_payer_id" in res:
        if 'LIBERTY MUTUAL' in res['vx_carrier_name'] and "WORKERS COMP" in res['vx_carrier_lob']:
            res['edi_payer_id'] = '33600'

    if "edi_payer_id" in res:
        if 'ALLSTATE' in res['vx_carrier_name'] and res['vx_carrier_lob'] == "AUTO (PERSONAL)":
            if res['vx_accident_state'] == 'NJ':
                res['edi_payer_id'] = 'C1089'
            else:
                res['edi_payer_id'] = 'C1037'

    #----
    # correct missing insured first name
    #----
    if not res['vx_carrier_insured_first_name'].strip():
        if 'PERSONAL' in res['vx_carrier_lob']:
            if res['patient_firstname'] in res['vx_carrier_insured_last_name'] and res['patient_lastname'] in res['vx_carrier_insured_last_name']:
                print('{}: missing insured firstname-- {},{} (changed to {},{})'.format(res['vx_pm_sk'], res['vx_carrier_insured_first_name'], res['vx_carrier_insured_last_name']
                                                                        , res['patient_firstname'], res['patient_lastname']), file=sys.stderr)

                res['vx_carrier_insured_first_name'] = res['patient_firstname']
                res['vx_carrier_insured_last_name'] = res['patient_lastname']

            elif res['vx_carrier_patient_first_name'] in res['vx_carrier_insured_last_name'] and res['vx_carrier_patient_last_name'] in res['vx_carrier_insured_last_name']:
                print('{}: missing insured firstname-- {},{} (changed to {},{})'.format(res['vx_pm_sk'], res['vx_carrier_insured_first_name'], res['vx_carrier_insured_last_name']
                                                                        , res['vx_carrier_patient_first_name'], res['vx_carrier_patient_last_name']), file=sys.stderr)

                res['vx_carrier_insured_first_name'] = res['vx_carrier_patient_first_name']
                res['vx_carrier_insured_last_name'] = res['vx_carrier_patient_last_name']
        
            elif res['vx_claim_type'] == 'FP':
                    print('{}: missing insured firstname-- {},{} (changed to {},{})'.format(res['vx_pm_sk'], res['vx_carrier_insured_first_name'], res['vx_carrier_insured_last_name']
                                                                        , res['patient_firstname'], res['patient_lastname']), file=sys.stderr)

                    res['vx_carrier_insured_first_name'] = res['patient_firstname']
                    res['vx_carrier_insured_last_name'] = res['patient_lastname']
        
            else:
                if len(res['vx_carrier_insured_last_name'].split()) == 1:
                    patfirstname = ''
                    patlastname = res['vx_carrier_insured_last_name']
                else:
                    try:
                        patfirstname = re.search(r'^\w+(?=\s)', res['vx_carrier_insured_last_name']).group() 
                    except:
                        patfirstname = ''
                    try:
                        patlastname = re.search(r'(?<=\s)\w+(?=\s&)|\w+$', res['vx_carrier_insured_last_name']).group() 
                    except:
                        patlastname = ''

                print('{}: missing insured firstname-- {},{} (changed to [{},{}])'.format(res['vx_pm_sk'], res['vx_carrier_insured_first_name'], res['vx_carrier_insured_last_name']
                                                                        , patfirstname, patlastname), file=sys.stderr)

                res['vx_carrier_insured_first_name'] = patfirstname 
                res['vx_carrier_insured_last_name'] = patlastname 

    if res['vx_carrier_insured_first_name'].strip(): pass
    elif re.search(r'COMMERC|EMPLOYER|STATE', res['vx_carrier_lob'], re.I): pass
    else: print('{}: missing insured firstname-- {},{}\n'.format(x, '', res['vx_carrier_insured_last_name']))#; sys.exit(1)
    
    #----
    # correct others
    #----
    if res['pay_to_addr1'].strip(): pass
    else: print(' !! missing Remittance info, pm_sk: {}'.format(x)); sys.exit(1)

    if re.match(r'TOKIO MARINE AMERICA INS', res["vx_carrier_name"], re.I):
       res["vx_carrier_policy_number"] = re.sub(r'^0+', '', res["vx_carrier_policy_number"])

    if res['patient_addr1'] == '9001 W ESECO RD':
        res['patient_addr1'] = '9001 W ESECO'

    if res['vx_pm_sk'] == '2477837': 
        res['patient_addr1'] = '59 ROMEYN AVE'
    
    plate = {    # SPECIFY
                'BOARD OF COUNTY COMMISSIONERSOF GARRETT': 'GARRETT CTY COMMISSIONERS BRD'
                , 'HOLIDAY BEACH RESORT SOUNDSIDE CONDOMINIUM ASSN INC': 'HOLIDAY BEACH RESORT SOUNDSIDE' 
                , 'ENTERPRISE': 'ENTERPRISE RENT-A-CAR'
            }

    for k, v in plate.items():
        if res['vx_carrier_insured_last_name'] == k: 
            #print(k, file=sys.stderr)
            res['vx_carrier_insured_last_name'] = v 


def check_edi_id(res):
    if "edi_payer_id" not in res:
        print("{} : no payer id".format( res['vx_pm_sk'] ))
        #sys.exit(0)


def check_dx_desciption(res):
    if res['claim_type'] != '837P':
        raise Exception("not 837P")

    if 'dx_code_desc' in res:
        diag_code_l = [code.strip() for code in res['diagnoses'].split(',')]
        diagdsc_code_l = [codeline.split('|')[0] for codeline in res['dx_code_desc'].split('@')]
        #print(diag_codes, diagdsc_codes, file=sys.stderr)

        diffl = list(set(diag_code_l).difference(set(diagdsc_code_l)))

        if len(diffl) != 0:
            print('{} : no dx_code_desc full | "diagnoses": "{}" missing'.format(res['vx_pm_sk'], ','.join(diffl)), file=sys.stderr)
    else:
        print('{} : no dx_code_desc | "diagnoses": "{}"'.format(res['vx_pm_sk'], res['diagnoses']), file=sys.stderr)
        #sys.exit(0)


def check_physician_taxo(res):
    if res['claim_type'] == '837P':
        print('{}: {},{}'.format(x, res['render_npi'], res['render_taxonomy']))
    elif res['claim_type'] == '837I':
        print('{}: {},{}'.format(x, res['attending_physician_npi'], res['attending_physician_taxonomy']))


def check_resend_marker(res):
    cnt = 0 
    
    if res['claim_frequency'] == '1':
        print(f"""{res['vx_pm_sk']} -- "claim frequency": {res['claim_frequency']} (changed to 7)""", file=sys.stderr)

        res['claim_frequency'] = '7'

        cnt += 1
    
    

def check_institutional_statement_period(res):
    if res['claim_type'] != '837I':
        raise Exception("not 837I")

    my_dates = {}
    for i in range(1, 2000):
        prefix = 'LX{:03}_'.format(i)
        svc_date = prefix + 'service_date'

        if svc_date not in res:
            break

        date = res[svc_date].strip()

        if date:
            my_dates[date] = 1
    #print(my_dates)

    #--------------------------------
    # procedure date is pasted to procedure, like Aultman
    #--------------------------------
    all_procedures = []
    if 'principal_procedure' in res and res['principal_procedure']:
        all_procedures.append(res['principal_procedure'])
    if 'other_procedures' in res and res['other_procedures']:
        all_procedures.append(res['other_procedures'])
    #print(all_procedures)

    for my_procedures in all_procedures:
        procedures = my_procedures.split(',')
        for prox in procedures:
            if ':' in prox:
                name, date = prox.split(':')
                date = date.strip()
                if date:
                    my_dates[date] = 1

    mydate = sorted(my_dates.keys())
    try:
        myperiod = '{}-{}'.format(mydate[0], mydate[-1])
    except:
        return     # some record without service date
    else:
        if myperiod != res['statement_period']:
            print('{}: statement_period {} -> LX / procedures fall in {}'.format(x, res['statement_period'], myperiod))


for s in sys.stdin:
    if not re.match(r'\s{0,6}{', s):
        continue

    res = json.loads(s)
    x = res['vx_pm_sk']


    check_resend_marker(res); print(json.dumps(res))                                           # check claim freq marker for resend

    #------------------------------------------
    # for missing insured zip code correction
    #------------------------------------------
    #check_quotes_clean(res); correct_common_errors(res); print(json.dumps(res))                # check missing insured first name 
    #check_insured_fullname(res); print(json.dumps(res))                                        # check insured first and last name with "&"
    #print_no_zip_address(res); print_commercial_insurance_holder(res)                          # check no zip code; check too long insured last name 
    #check_state_zip_length(res)

    #----------
    # unmark following choices based on the file content
    #----------
    #check_dx_desciption(res)                                                                   # only for Prof 
    #check_institutional_statement_period(res)                                                  # only for Inst 
    #check_fax(res)                                                                             # only for Fax
    #check_edi_id(res)                                                                          # only for EDI

    #check_lockbox(res)
    #check_balance(res)
    #print('{}| {}, {}'.format(res['vx_pm_sk'], res['facility_npi'], res['facility']))          # only for Prof
    #check_physician_taxo(res)
    #check_claim_party(res)                                                                     # patient_addr1 PO BOX valid for fax only, vx_carrier_insured_address_1 PO BOX valid for any



