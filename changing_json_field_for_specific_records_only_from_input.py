#! /usr/bin/python3
import os, sys
import json
import re


jar = {}

with open('template.txt', 'r') as fr:
    for line in fr:
        line = line.strip()
        
        if not re.match(r'^\d{7}: missing insured firstname', line):
            continue

        if '[' in line or ']' in line:
            ky = re.search(r'^\d{7}', line).group()
            jar[ky] = 1


cnt = 0

for line in sys.stdin:
    dicy = json.loads(line)
    
    try:
        if dicy['edi_payer_id']:
            ind = 'xx' 
    except:
        ind = 'yy' 

    if dicy['vx_pm_sk'] in jar:    # change only for pm_sks in template
    
        msg = """\
{}: missing insured firstname-- ,{}
[james.niu@revproc01 jsondump]$ grep {} {}0.json | jsonl | grep lob
    "vx_carrier_lob": "{}", (changed to AUTO (COMMERCIAL))"""
        print(msg.format(dicy['vx_pm_sk'], dicy['vx_carrier_insured_last_name'], dicy['vx_pm_sk'], ind, dicy['vx_carrier_lob']), file=sys.stderr) 

        dicy['vx_carrier_lob'] = 'AUTO (COMMERCIAL)'
           
        cnt += 1
            
    print(json.dumps(dicy))
    
