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

package org.apache.slider.providers.accumulo.funtest

import org.apache.hadoop.conf.Configuration
import org.apache.slider.funtest.abstracttests.AbstractTestBuildSetup
import org.junit.Test

class AccumuloBuildSetupIT extends AbstractTestBuildSetup {

  @Test
  public void testAccumuloBuildsHavePathsDefined() throws Throwable {
    Configuration conf = loadSliderConf();

    assumeBoolOption(conf, KEY_TEST_ACCUMULO_ENABLED, true)

    assertStringOptionSet(conf, KEY_TEST_ACCUMULO_APPCONF)
    assertStringOptionSet(conf, KEY_TEST_ACCUMULO_TAR)
  }

}
