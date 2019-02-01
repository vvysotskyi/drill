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
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableMetadata implements BaseMetadata {
  private final String tableName;
  private final String location;
  private final TupleSchema schema;
  private final Map<SchemaPath, ColumnStatistic> columnStatistics;
  private final Map<String, Object> tableStatistics;
  private final long lastModifiedTime;
  private final String owner;
  private final Set<String> partitionKeys;
  private List<FileMetadata> files;
//  private List<FileMetadata> partitions;
//  private boolean supportsGroupScan;

  public static final TableMetadata EMPTY = new TableMetadata();

  public TableMetadata(String tableName, String location, TupleSchema schema,
                       Map<SchemaPath, ColumnStatistic> columnStatistics, Map<String, Object> tableStatistics, long lastModifiedTime, String owner, Set<String> partitionKeys) {
    this.tableName = tableName;
    this.location = location;
    this.schema = schema;
    this.columnStatistics = columnStatistics;
    this.tableStatistics = tableStatistics;
    this.lastModifiedTime = lastModifiedTime;
    this.owner = owner;
    this.partitionKeys = partitionKeys;
  }

  private TableMetadata() {
    tableName = null;
    location = null;
    schema = new TupleSchema();
    columnStatistics = Collections.emptyMap();
    tableStatistics = Collections.emptyMap();
    lastModifiedTime = -1;
    owner = null;
    partitionKeys = Collections.emptySet();
  }

  public Object getStatisticsForColumn(SchemaPath columnName, StatisticsKind statisticsKind) {
    return columnStatistics.get(columnName).getStatistic(statisticsKind);
  }

  public ColumnStatistic getColumnStats(SchemaPath columnName) {
    return columnStatistics.get(columnName);
  }

  public Object getStatistic(StatisticsKind statisticsKind) {
    return tableStatistics.get(statisticsKind.getName());
  }

  public ColumnMetadata getColumn(SchemaPath name) {
    return SchemaPathUtils.getColumnMetadata(name, schema);
  }

  public TupleSchema getSchema() {
    return schema;
  }

  public boolean isPartitionColumn(String fieldName) {
    return partitionKeys.contains(fieldName);
  }

  boolean isPartitioned() {
    return !partitionKeys.isEmpty();
  }

  public String getTableName() {
    return tableName;
  }

  public String getLocation() {
    return location;
  }

  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public String getOwner() {
    return owner;
  }

  public List<FileMetadata> getFiles() {
    return files;
  }

  public Map<SchemaPath, ColumnStatistic> getColumnStatistics() {
    return columnStatistics;
  }
}
