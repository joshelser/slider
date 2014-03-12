#!/usr/bin/env python

'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import json
import os.path
import logging
from datetime import datetime
import pprint
from ambari_agent import AgentException

HOSTS_LIST_KEY = "all_hosts"
PING_PORTS_KEY = "all_ping_ports"

logger = logging.getLogger()

# Converts from 1-3,5,6-8 to [1,2,3,5,6,7,8]
def convertRangeToList(list):
  resultList = []

  for i in list:

    ranges = i.split(',')

    for r in ranges:
      rangeBounds = r.split('-')
      if len(rangeBounds) == 2:

        if not rangeBounds[0] or not rangeBounds[1]:
          raise AgentException.AgentException(
            "Broken data in given range, expected - ""m-n"" or ""m"", got : " + str(r))

        resultList.extend(range(int(rangeBounds[0]), int(rangeBounds[1]) + 1))
      elif len(rangeBounds) == 1:
        resultList.append((int(rangeBounds[0])))
      else:
        raise AgentException.AgentException("Broken data in given range, expected - ""m-n"" or ""m"", got : " + str(r))

  return resultList

#Converts from ['1:0-2,4', '42:3,5-7'] to [1,1,1,42,1,42,42,42]
def convertMappedRangeToList(list):
  resultDict = {}

  for i in list:
    valueToRanges = i.split(":")
    if len(valueToRanges) <> 2:
      raise AgentException.AgentException(
        "Broken data in given value to range, expected format - ""value:m-n"", got - " + str(i))
    value = valueToRanges[0]
    rangesToken = valueToRanges[1]

    for r in rangesToken.split(','):

      rangeIndexes = r.split('-')

      if len(rangeIndexes) == 2:

        if not rangeIndexes[0] or not rangeIndexes[1]:
          raise AgentException.AgentException(
            "Broken data in given value to range, expected format - ""value:m-n"", got - " + str(r))

        start = int(rangeIndexes[0])
        end = int(rangeIndexes[1])

        for k in range(start, end + 1):
          resultDict[k] = int(value)


      elif len(rangeIndexes) == 1:
        index = int(rangeIndexes[0])

        resultDict[index] = int(value)

  resultList = dict(sorted(resultDict.items())).values()

  return resultList


def decompressClusterHostInfo(clusterHostInfo):
  info = clusterHostInfo.copy()
  #Pop info not related to host roles  
  hostsList = info.pop(HOSTS_LIST_KEY)
  pingPorts = info.pop(PING_PORTS_KEY)

  decompressedMap = {}

  for k, v in info.items():
    # Convert from 1-3,5,6-8 to [1,2,3,5,6,7,8] 
    indexes = convertRangeToList(v)
    # Convert from [1,2,3,5,6,7,8] to [host1,host2,host3...]
    decompressedMap[k] = [hostsList[i] for i in indexes]

  #Convert from ['1:0-2,4', '42:3,5-7'] to [1,1,1,42,1,42,42,42]
  pingPorts = convertMappedRangeToList(pingPorts)

  #Convert all elements to str
  pingPorts = map(str, pingPorts)

  #Add ping ports to result
  decompressedMap[PING_PORTS_KEY] = pingPorts
  #Add hosts list to result
  decompressedMap[HOSTS_LIST_KEY] = hostsList

  return decompressedMap


def main():
  pass


if __name__ == '__main__':
  main()

