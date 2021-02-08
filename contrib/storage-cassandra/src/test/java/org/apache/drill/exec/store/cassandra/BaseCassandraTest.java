/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.cassandra;

import com.github.nosan.embedded.cassandra.Cassandra;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.util.ComparableVersion;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assume.assumeTrue;

public class BaseCassandraTest extends ClusterTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestCassandraSuit.initCassandra();
    initCassandraPlugin(TestCassandraSuit.cassandra);
  }

  private static void initCassandraPlugin(Cassandra cassandra) throws Exception {
    assumeTrue(
        "Skipping tests for JDK 12+ since Cassandra supports only versions up to 11 (including).",
        new ComparableVersion(System.getProperty("java.version"))
            .compareTo(new ComparableVersion("12")) < 0);

    startCluster(ClusterFixture.builder(dirTestWatcher));

    CassandraStorageConfig config = new CassandraStorageConfig(
        cassandra.getSettings().getAddress().getHostAddress(),
        cassandra.getSettings().getPort(),
        null,
        null);
    config.setEnabled(true);
    cluster.defineStoragePlugin("cassandra", config);
  }

  @AfterClass
  public static void tearDownCassandra() {
    if (TestCassandraSuit.isRunningSuite()) {
      TestCassandraSuit.tearDownCluster();
    }
  }
}
