#!/bin/env python
# Polls the Condor collector for some data and outputs it to redis
# L.B. 21-Jun-2013

# Import some standard python utilities
import sys, time, argparse 
import classad, htcondor # requires condor 7.9.5+
import redis

## Parse our arguments
parser = argparse.ArgumentParser(description="Poll HTCondor collector for information and dump into redis")
parser.add_argument("collector", help="address of the HTCondor collector")
parser.add_argument("redserver", help="address of the Redis server") 
args = parser.parse_args()

# Connect to condor collector and grab some data
coll = htcondor.Collector(args.collector)
slotState = coll.query(htcondor.AdTypes.Startd, "true",['Name','RemoteGroup','NodeOnline','JobId','State','RemoteOwner','COLLECTOR_HOST_STRING'])

# Set our application id
appId = "calliope"

#Setup redis
r_server =redis.Redis(args.redserver)

# Let's set the timestamp outside of the loop, such that each time we run the 
# backend, all nodes report the same timestamp. 
timestamp = str(int(time.time()))
indexVal = []
# This forms the data structure and pushes it into redis as a list.
for slot in slotState[:]:
  key = appId + ":" + slot['Name'] + ":" + timestamp 
  if (slot['State'] == "Owner") or (slot['State'] == "Unclaimed"):  ## If slot is in owner state there is no RemoteOwner or RemoteGroup
    value = ["nil",slot['NodeOnline'],slot['State'],"nil","nil",slot['COLLECTOR_HOST_STRING']]
  else: 
    value = [slot['JobId'],slot['NodeOnline'],slot['State'],slot['RemoteOwner'],slot['RemoteGroup'],slot['COLLECTOR_HOST_STRING']]
  for entry in value:
      # we could probably make this faster by pipelining
      r_server.lpush(key,entry)
  # Add the list of keys to an index (now the keys are values O_o)
  # better naming needed..
  indexVal.append(key)

indexKey = appId + ":index:latest" # bad naming scheme or _worst_ name scheme?
# need some logic to check if the key exists
# suspect it will fail if the key does not initially exist
print indexKey
print r_server.exists(indexKey)
if r_server.exists(indexKey) == True: 
  r_server.lset(indexKey,0,indexVal)
else: 
  r_server.lpush(indexKey,indexVal)
