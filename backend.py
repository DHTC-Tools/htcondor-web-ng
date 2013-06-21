#!/bin/env python
# Polls the Condor collector for some data and outputs it to redis
# L.B. 21-Jun-2013

### notes:
# currently i can push about data for 1000 slots per second into redis)
# this is a nice number -- scaling the sampling rate is really easy
# but i'd love to see better performance.

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
  indexVal.append(key)

indexKey = appId + ":index:latest" # bad naming scheme or _worst_ name scheme?
# this is an inappropriate use of a list.
# because we concatinate the whole list into a single index.
# bad bad bad
if r_server.exists(indexKey) == True: 
  r_server.lset(indexKey,0,indexVal)
else: 
  r_server.lpush(indexKey,indexVal)
