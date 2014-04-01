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

import static org.apache.hoya.HoyaXMLConfKeysForTesting.KEY_TEST_ACCUMULO_APPCONF
import static org.apache.hoya.HoyaXMLConfKeysForTesting.KEY_TEST_ACCUMULO_TAR
import static org.apache.hoya.api.ResourceKeys.YARN_MEMORY
import static org.apache.hoya.providers.accumulo.AccumuloKeys.*
import static org.apache.hoya.yarn.Arguments.ARG_PROVIDER
import static org.apache.hoya.yarn.Arguments.ARG_RES_COMP_OPT

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hoya.funtest.framework.CommandTestBase
import org.apache.hoya.funtest.framework.SliderShell
import org.apache.hoya.providers.accumulo.AccumuloKeys
import org.apache.hoya.yarn.Arguments
import org.junit.Before

/**
 * Anything specific to accumulo tests
 */
abstract class AccumuloCommandTestBase extends CommandTestBase {

  @Before
  public void verifyPreconditions() {

    //if tests are not enabled: skip tests
    assumeAccumuloTestsEnabled()
    // but if they are -fail if the values are missing
    getRequiredConfOption(HOYA_CONFIG, OPTION_ZK_HOME)
    getRequiredConfOption(HOYA_CONFIG, OPTION_HADOOP_HOME)
  }

  /**
   * Create an accumulo cluster
   *
   * @param clustername
   * @param roles
   * @param argsList
   * @param blockUntilRunning
   * @param containerMemory
   * @return
   */
  public SliderShell createAccumuloCluster(String clustername,
                                         Map<String, Integer> roles,
                                         List<String> argsList,
                                         boolean blockUntilRunning,
                                         Map<String, String> clusterOps,
                                         String containerMemory,
                                         String password) {
    argsList << ARG_PROVIDER << PROVIDER_ACCUMULO;


    YarnConfiguration conf = HOYA_CONFIG
    clusterOps[OPTION_ZK_HOME] = getRequiredConfOption(
        HOYA_CONFIG, OPTION_ZK_HOME)
    clusterOps[OPTION_HADOOP_HOME] = getRequiredConfOption(
        HOYA_CONFIG,
        OPTION_HADOOP_HOME)
    argsList << Arguments.ARG_IMAGE <<
    getRequiredConfOption(HOYA_CONFIG, KEY_TEST_ACCUMULO_TAR)

    argsList << Arguments.ARG_CONFDIR <<
    getRequiredConfOption(HOYA_CONFIG, KEY_TEST_ACCUMULO_APPCONF)
    
    argsList << Arguments.ARG_OPTION << AccumuloKeys.OPTION_ACCUMULO_PASSWORD << password

    argsList << ARG_RES_COMP_OPT << ROLE_MASTER <<
    YARN_MEMORY << containerMemory
    argsList << ARG_RES_COMP_OPT << ROLE_TABLET <<
    YARN_MEMORY << containerMemory
    argsList << ARG_RES_COMP_OPT << ROLE_MONITOR <<
    YARN_MEMORY << containerMemory
    argsList << ARG_RES_COMP_OPT << ROLE_GARBAGE_COLLECTOR <<
    YARN_MEMORY << containerMemory

    return createHoyaCluster(clustername,
                             roles,
                             argsList,
                             blockUntilRunning,
                             clusterOps)
  }
}
