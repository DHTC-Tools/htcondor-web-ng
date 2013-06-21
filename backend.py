#!/bin/env python
# Polls the Condor collector for some data and outputs it to redis
# data structure is something like slot@host = [list,of,some,values] 
# L.B. 17-Jun-2013

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
_tmpKey = str(int(time.time()))
# This forms the data structure and pushes it into redis as a list.
for slot in slotState[:]:
  key = appId + ":" + slot['Name'] + ":" + _tmpKey
  if (slot['State'] == "Owner") or (slot['State'] == "Unclaimed"):  ## If slot is in owner state there is no RemoteOwner or RemoteGroup
    value = ["nil",slot['NodeOnline'],slot['State'],"nil","nil",slot['COLLECTOR_HOST_STRING']]
  else: 
    value = [slot['JobId'],slot['NodeOnline'],slot['State'],slot['RemoteOwner'],slot['RemoteGroup'],slot['COLLECTOR_HOST_STRING']]
  for entry in value:
      # we could probably make this faster by pipelining
      r_server.lpush(key,entry)
