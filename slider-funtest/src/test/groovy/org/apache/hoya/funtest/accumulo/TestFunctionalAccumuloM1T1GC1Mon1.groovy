/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.util.Map;

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import org.apache.hadoop.conf.Configuration;
import org.apache.hoya.HoyaExitCodes
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.funtest.framework.FuntestProperties
import org.apache.hoya.funtest.framework.PortAssignments
import org.apache.hoya.yarn.Arguments
import org.apache.hoya.yarn.HoyaActions
import org.apache.hoya.yarn.client.HoyaClient
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

import static org.apache.hoya.providers.accumulo.AccumuloKeys.*
import static org.apache.hoya.providers.accumulo.AccumuloConfigFileOptions.*

@CompileStatic
@Slf4j
public class TestFunctionalAccumuloM1T1GC1Mon1 extends TestFunctionalAccumuloCluster {

  @Override
  public String getClusterName() {
    return "test_functional_accumulo_m1t1gc1mon1";
  }

  /**
   * Override point for any cluster load operations
   * @param clientConf
   * @param numWorkers
   */
  @Override
  public void clusterLoadOperations(
      String clustername,
      Configuration clientConf,
      int numWorkers,
      Map<String, Integer> roleMap,
      ClusterDescription cd) {

    hoya(0, [
      HoyaActions.ACTION_FREEZE,
      getClusterName(),
      Arguments.ARG_WAIT,
      Integer.toString(FREEZE_WAIT_TIME),
      Arguments.ARG_MESSAGE,
      "freeze-in-test-AccumuloCluster"
    ])
    
    //destroy the cluster. This only works if the permissions allow it
    destroy(0, getClusterName())
  }
}
