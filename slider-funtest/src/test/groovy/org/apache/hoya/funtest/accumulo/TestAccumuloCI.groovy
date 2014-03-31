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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hoya.api.ClusterDescription
import org.apache.hoya.funtest.framework.CommandTestBase
import org.apache.hoya.funtest.framework.FuntestProperties


/**
 * 
 */
@CompileStatic
@Slf4j
class TestAccumuloCI extends TestFunctionalAccumuloCluster {
  
  @Override
  String getClusterName() {
    return "test_accumulo_ci"
  }

  @Override
  void clusterLoadOperations(
      String clustername,
      Map<String, Integer> roleMap,
      ClusterDescription cd) {
    assert clustername
    int ret = 0;

    String zookeepers = CommandTestBase.HOYA_CONFIG.get(FuntestProperties.KEY_HOYA_TEST_ZK_HOSTS, FuntestProperties.DEFAULT_HOYA_ZK_HOSTS)
    ZooKeeperInstance inst = new ZooKeeperInstance(System.getProperty("user.name") + "-" + clustername, zookeepers)
    PasswordToken passwd = new PasswordToken(getPassword())
    Connector conn = inst.getConnector("root", new PasswordToken(getPassword()))
    
    String tableName = "testAccumuloCi";
    conn.tableOperations().create(tableName);

    assert ret == 0;
  }
}
