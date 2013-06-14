#!/bin/env python
# This code reads a Condor classads and prints the number of jobs running for
# each accounting group. This is then converted into json format for plotting
# purposes. This is, at best, a very ugly prototype
#
# Lincoln Bryant
# 14-Jun-2013 

# Requires HTCondor 7.9.5+
import htcondor
import classad

import json

# Define HTCondor collector and the associated Schedd
# the collector has to have some awareness of this schedd in order to work  
coll = htcondor.Collector("uct2-condor.mwt2.org")
scheddAd = coll.locate(htcondor.DaemonTypes.Schedd,"uct2-gk.mwt2.org")
schedd = htcondor.Schedd(scheddAd)

# Query the schedd for all AccountingGroup class ads and build a list
# MUCH faster than using vanilla condor_q parsing -- Thanks Brian!
jobs = schedd.query('true',['AccountingGroup'])
groupList = []
for job in jobs[:]:
  groupList.append(job['AccountingGroup'])

# Determine the unique accounting groups, then build a dictionary such that the key is the unique list name
# and the value is the number of occurances. Then dump to JSON format
d = {}
for group in list(set(groupList)): 
  d[group] = groupList.count(group)
print json.dumps(d)
