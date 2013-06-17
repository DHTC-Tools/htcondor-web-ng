#!/bin/env python
# Polls the Condor collector for some data and outputs it to redis
# data structure is something like slot@host = [list,of,some,values] 
# L.B. 17-Jun-2013

# Import some standard python utilities
import sys, time, argparse 
import htcondor, classad # requires condor 7.9.5+
import redis

## Parse our arguments
parser = argparse.ArgumentParser(description="Poll HTCondor collector for information and dump into redis")
parser.add_argument("collector", help="address of the HTCondor collector")
parser.add_argument("redserver", help="address of the Redis server") 
args = parser.parse_args()

# Connect to condor collector and grab some data
coll = htcondor.Collector(args.collector)
slotState = coll.query(htcondor.AdTypes.Startd, "true",['Name','RemoteGroup','NodeOnline','JobId','State','RemoteOwner','COLLECTOR_HOST_STRING'])

#Setup redis
r_server =redis.Redis(args.redserver)

# This forms the data structure and pushes it into redis as a list 
for slot in slotState[:]:
  key = slot['Name']
  if (slot['State'] == "Owner") or (slot['State'] == "Idle"):  ## If slot is in owner state there is no RemoteOwner or RemoteGroup
    value = ["nil",slot['NodeOnline'],slot['State'],"nil","nil",slot['COLLECTOR_HOST_STRING']]
  else: 
    value = [slot['JobId'],slot['NodeOnline'],slot['State'],slot['RemoteOwner'],slot['RemoteGroup'],slot['COLLECTOR_HOST_STRING']]
### this is wrong. when the code runs again, the new entries will get appended 
### onto the old ones. Instead, we need to update the entries.
  for entry in value:
	r_server.lpush(slot['Name'],entry)
