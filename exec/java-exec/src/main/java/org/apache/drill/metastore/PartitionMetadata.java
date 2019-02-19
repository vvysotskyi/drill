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

// actually this class represents not a partition metadata,
// but a metadata for table part which corresponds to the concrete partition key.
// Therefore such schema as Map<String, Object> values should be removed.
public class PartitionMetadata implements BaseMetadata {
  private final SchemaPath column;
  // part location -> part column value
//  private final Map<String, Object> values;
  // TODO: currently it is impossible to obtain statistics for the concrete partition.
  // Refactor this code to allow that.
  private final TupleSchema schema;
  private final Map<SchemaPath, ColumnStatistic> columnStatistics;
  private final Map<String, Object> partitionStatistics;
  // TODO: decide which of these: fileName or location should be left.
  private final Set<String> location;
  // TODO: decide whether this field is required
  private final String tableName;
  private final long lastModifiedTime;

  public PartitionMetadata(SchemaPath column,
//      Map<String, Object> values,
                           TupleSchema schema,
      Map<SchemaPath, ColumnStatistic> columnStatistics,
      Map<String, Object> partitionStatistics,
      Set<String> location,
      String tableName,
      long lastModifiedTime) {
    this.column = column;
//    this.values = values;
    this.schema = schema;
    this.columnStatistics = columnStatistics;
    this.partitionStatistics = partitionStatistics;
    this.location = location;
    this.tableName = tableName;
    this.lastModifiedTime = lastModifiedTime;
  }

  public SchemaPath getColumn() {
    return column;
  }

//  public Map<String, Object> getValues() {
//    return values;
//  }

  public Set<String> getLocations() {
    return location;
  }

  public String getTableName() {
    return tableName;
  }

  public long getLastModifiedTime() {
    return lastModifiedTime;
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
  public Map<SchemaPath, ColumnStatistic> getColumnStatistics() {
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
}