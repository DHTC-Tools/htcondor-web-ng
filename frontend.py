#!/bin/env python
# Polls redis for data and prints it in JSON format 
# L.B. 21-Jun-2013

# Import some standard python utilities
import sys, time, argparse 
import redis, json

## Parse our arguments
parser = argparse.ArgumentParser(description="Convert redis dump to json ")
parser.add_argument("redserver", help="address of the Redis server") 
args = parser.parse_args()

# Set our application id
appId = "calliope"

#Setup redis
r_server =redis.Redis(args.redserver)

#get some values
# 
slotIndex = r_server.smembers(appId+":index")
latestTimestamp = r_server.lpop(appId+":index:latest:timestamp")
for uniqId in slotIndex:
  _key = uniqId + ":" + latestTimestamp
  data = r_server.lrange(_key,0,6) # we just use 6 as a magic number here. bad!
  ## need to massage this data into json
  # a task for monday, perhaps 
