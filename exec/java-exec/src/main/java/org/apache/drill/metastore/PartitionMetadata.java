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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PartitionMetadata implements BaseMetadata {
  private final String columnName;
  private final List<String> values;
  // TODO: currently it is impossible to obtain statistics for the concrete partition.
  // Refactor this code to allow that.
  private final LinkedHashMap<SchemaPath, TypeProtos.MajorType> fields;
  private final Map<SchemaPath, ColumnStatistic> columnStatistics;
  private final Map<String, Object> partitionStatistics;
  // TODO: decide which of these: fileName or location should be left.
  private final String location;
  // TODO: decide whether this field is required
  private final String tableName;
  private final long lastModifiedTime;
  // TODO: move common for file and row group info to the base class and create a separate class for row group
  private List<FileMetadata> rowGroups;

  public PartitionMetadata(String columnName, List<String> values,
                           LinkedHashMap<SchemaPath, TypeProtos.MajorType> fields, Map<SchemaPath, ColumnStatistic> columnStatistics, Map<String, Object> partitionStatistics, String location, String tableName, long lastModifiedTime) {
    this.columnName = columnName;
    this.values = values;
    this.fields = fields;
    this.columnStatistics = columnStatistics;
    this.partitionStatistics = partitionStatistics;
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

  public String getLocation() {
    return location;
  }

  public String getTableName() {
    return tableName;
  }

  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public TypeProtos.MajorType getField(SchemaPath name) {
    return fields.get(name);
  }

  public Map<SchemaPath, TypeProtos.MajorType> getFields() {
    return fields;
  }

  public Map<SchemaPath, ColumnStatistic> getColumnStatistics() {
    return columnStatistics;
  }

  public Object getStatistic(StatisticsKind statisticsKind) {
    return partitionStatistics.get(statisticsKind.getName());
  }
}
