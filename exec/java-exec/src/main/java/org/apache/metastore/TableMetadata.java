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


import com.google.common.collect.Table;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.LinkedHashMap;
import java.util.Set;

public abstract class TableMetadata {
  String tableName;
  String location;
  LinkedHashMap<String, RelDataTypeField> fields;
  Table<String, String, Object> columnStatistics; // Guava’s HashBasedTable
  long lastModifiedTime;
  String owner;
  private Set<String> partitionKeys;

  abstract Object getStatisticsForColumn(String columnName, StatisticsKind statisticsKind);

  abstract RelDataTypeField getField(String name);

  abstract boolean isPartitionColumn(String fieldName);

  boolean isPartitioned() {
    return !partitionKeys.isEmpty();
  }

}
