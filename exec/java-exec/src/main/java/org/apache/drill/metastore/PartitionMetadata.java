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
package org.apache.drill.metastore;

import org.apache.drill.shaded.guava.com.google.common.collect.Table;

import java.util.List;

public class PartitionMetadata {
  private final String columnName;
  private final List<String> values;
  // TODO: currently it is impossible to obtain statistics for the concrete partition.
  // Refactor this code to allow that.
  private final Table<String, String, Object> columnStatistics; // Guava’s HashBasedTable
  private final String location;
  private final String tableName;
  private final long lastModifiedTime;

  public PartitionMetadata(String columnName, List<String> values,
      Table<String, String, Object> columnStatistics,String location, String tableName, long lastModifiedTime) {
    this.columnName = columnName;
    this.values = values;
    this.columnStatistics = columnStatistics;
    this.location = location;
    this.tableName = tableName;
    this.lastModifiedTime = lastModifiedTime;
  }

  public String getColumnName() {
    return columnName;
  }

  public List<String> getValues() {
    return values;
  }

  public Object getStatisticsForColumn(String columnName, StatisticsKind statisticsKind) {
    return columnStatistics.get(columnName, statisticsKind.getName());
  }

  public String getLocation() {
    return location;
  }

  public String getTableName() {
    return tableName;
  }

  public long getLastModifiedTime() {
    return lastModifiedTime;
  }
}
