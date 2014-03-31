/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hoya.funtest.accumulo

import org.apache.hadoop.conf.Configuration
import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.funtest.framework.FuntestProperties
import org.apache.hoya.funtest.framework.PortAssignments
import org.apache.hoya.tools.ConfigHelper
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.After
import org.junit.Before
import org.junit.Test

/**
 * 
 */
class TestFunctionalAccumuloCluster extends AccumuloCommandTestBase
    implements FuntestProperties, Arguments, HoyaExitCodes {

      
  public String getClusterName() {
    return "test_functional_accumulo_cluster"
  }
      
  @Before
  public void prepareCluster() {
    setupCluster(getClusterName())
  }

  @After
  public void destroyCluster() {
    teardown(getClusterName())
  }

  public int getNumMasters() {
    return 1
  }
  
  public int getNumTservers() {
    return 1
  }
  
  public int getNumMonitors() {
    return 1
  }
  
  public int getNumGarbageCollectors() {
    return 1
  }
  
  public int getNumTracers() {
    return 0
  }

  @Test
  public void testAccumuloClusterCreate() throws Throwable {

    describe "Create a working Accumulo cluster"

    def path = buildClusterPath(getClusterName())
    assert !clusterFS.exists(path)

    Map<String, Integer> roleMap = [
      (ROLE_MASTER) : getNumMasters(),
      (ROLE_TABLET) : getNumTservers(),
      (ROLE_MONITOR): getNumMonitors(),
      (ROLE_GARBAGE_COLLECTOR): getNumGarbageCollectors(),
      (ROLE_TRACER) : getNumTracers()
    ];

    Map<String, String> clusterOps = [:]
    clusterOps["site." + MONITOR_PORT_CLIENT] =
        Integer.toString (PortAssignments._test_functional_accumulo_m1t1gc1mon1_mon)

    List<String> extraArgs = []

    createAccumuloCluster(
        getClusterName(),
        roleMap,
        extraArgs,
        true,
        clusterOps,
        "256"
        )

    //get a hoya client against the cluster
    HoyaClient hoyaClient = bondToCluster(HOYA_CONFIG, getClusterName())
    ClusterDescription cd = hoyaClient.clusterDescription
    assert getClusterName() == cd.name

    log.info("Connected via HoyaClient {}", hoyaClient.toString())

    //wait for the role counts to be reached
    waitForRoleCount(hoyaClient, roleMap, ACCUMULO_LAUNCH_WAIT_TIME)
    
    clusterLoadOperations(clusterName, clientConf, numWorkers, roleMap, cd)
  }


  public String getDescription() {
    return "Create a working Accumulo cluster $clusterName"
  }

  /**
   * Override point for any cluster load operations
   * @param clientConf
   * @param numWorkers
   */
  public void clusterLoadOperations(
      String clustername,
      Configuration clientConf,
      int numWorkers,
      Map<String, Integer> roleMap,
      ClusterDescription cd) {

    log.info("Client Configuration = " + ConfigHelper.dumpConfigToString(clientConf))
  }

}
