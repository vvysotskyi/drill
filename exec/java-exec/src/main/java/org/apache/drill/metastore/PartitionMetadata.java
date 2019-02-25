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

import java.util.Map;
import java.util.Set;

/**
 * Represents a metadata for table part which corresponds to the concrete partition key.
 */
public class PartitionMetadata implements BaseMetadata {
  private final SchemaPath column;
  private final TupleSchema schema;
  private final Map<SchemaPath, ColumnStatistics> columnStatistics;
  private final Map<String, Object> partitionStatistics;
  private final Set<String> location;
  private final String tableName;
  private final long lastModifiedTime;

  public PartitionMetadata(SchemaPath column,
                           TupleSchema schema,
                           Map<SchemaPath, ColumnStatistics> columnsStatistics,
                           Map<String, Object> partitionStatistics,
                           Set<String> location,
                           String tableName,
                           long lastModifiedTime) {
    this.column = column;
    this.schema = schema;
    this.columnStatistics = columnsStatistics;
    this.partitionStatistics = partitionStatistics;
    this.location = location;
    this.tableName = tableName;
    this.lastModifiedTime = lastModifiedTime;
  }

  @Override
  public ColumnMetadata getColumn(SchemaPath name) {
    return SchemaPathUtils.getColumnMetadata(name, schema);
  }

  @Override
  public TupleSchema getSchema() {
    return schema;
  }

  @Override
  public Map<SchemaPath, ColumnStatistics> getColumnsStatistics() {
    return columnStatistics;
  }

  @Override
  public Object getStatistic(StatisticsKind statisticsKind) {
    return partitionStatistics.get(statisticsKind.getName());
  }

  @Override
  public Object getStatisticsForColumn(SchemaPath columnName, StatisticsKind statisticsKind) {
    return columnStatistics.get(columnName).getStatistic(statisticsKind);
  }

  public SchemaPath getColumn() {
    return column;
  }

  public Set<String> getLocations() {
    return location;
  }

  public String getTableName() {
    return tableName;
  }

  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

}
