#! /usr/bin/python3
import sys
import json
import re


change = {}

with open('new_party.txt', 'r') as fh:
    for line in fh:
        if re.match(r'^\s*$', line):
            continue
        tmp = line.strip().split()
        change[tmp[0]] = tmp[1]


for line in sys.stdin:
    if re.match(r'^\s*$', line):
       continue

    jar = json.loads(line)
    if jar['vx_pm_sk'] in change:
        if jar['vx_claim_type'] == 'TP':
            jar['vx_claim_type'] = 'FP'
        else:
            jar['vx_claim_type'] = 'TP'

    r = json.dumps(jar)
    print(r)

