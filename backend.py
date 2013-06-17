#!/bin/env python
# Polls the Condor collector for some data and outputs it to redis
# data structure is something like slot@host = [list,of,some,values] 
# L.B. 17-Jun-2013

# Import some standard python utilities
import sys, time, argparse 

# Requires HTCondor 7.9.5+
import htcondor, classad


## Parse our arguments
parser = argparse.ArgumentParser(description="Poll HTCondor collector for information and dump into redis")
parser.add_argument("collector", help="address of the HTCondor collector")
args = parser.parse_args()

coll = htcondor.Collector(args.collector)
results = coll.query(htcondor.AdTypes.Startd, "true", ["Name"])
print len(results)
