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
package org.apache.metastore;

import org.apache.drill.exec.physical.impl.window.Partition;

import java.util.List;

public interface Metastore {
  boolean addTable(TableMetadata table);

  TableMetadata getTableMetadata(String location, String tableName);

  boolean dropTableMetadata(String location, String tableName);

  boolean alterTableMetadata(TableMetadata newMetadata);

  boolean tableExists(String location, String tableName);

  boolean addPartition(String tableLocation, String tableName, PartitionMetadata partitionMeta);

  Partition getPartitionMetadata(String location, String tableName, String columnName);

  boolean dropPartitionMetadata(String location, String tableName, String columnName, List<String> partitionValues);

  // the same for FileMetadata

  void initMetastore();

  void dropMetastore();

  void setTimeout(long millis);  // 0 - no timeout
}
