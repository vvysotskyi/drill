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
import org.apache.drill.exec.record.metadata.TupleSchema;

import java.util.Map;

/**
 * Common statistics provider for table, partition, file or row group.
 */
public interface BaseMetadata {

  /**
   * Returns statistics stored in current metadata represented
   * as Map of column {@code SchemaPath}s and corresponding {@code ColumnStatistics}.
   *
   * @return statistics stored in current metadata
   */
  Map<SchemaPath, ColumnStatistics> getColumnsStatistics();

  /**
   * Returns statistics for specified column stored in current metadata.
   *
   * @param columnName column whose statistics should be returned
   * @return statistics for specified column
   */
  ColumnStatistics getColumnStatistics(SchemaPath columnName);

  /**
   * Returns schema stored in current metadata represented as
   * {@link TupleSchema}.
   *
   * @return schema stored in current metadata
   */
  TupleSchema getSchema();

  /**
   * Returns value of non-column statistics which corresponds to specified {@link StatisticsKind}.
   *
   * @param statisticsKind statistics kind whose value should be returned
   * @return value of non-column statistics
   */
  Object getStatistic(StatisticsKind statisticsKind);

  /**
   * Returns value of column statistics which corresponds to specified {@link StatisticsKind}
   * for column with specified {@code columnName}.
   *
   * @param columnName     name of the column
   * @param statisticsKind statistics kind whose value should be returned
   * @return value of column statistics
   */
  Object getStatisticsForColumn(SchemaPath columnName, StatisticsKind statisticsKind);

  /**
   * Returns metadata description for the specified column
   *
   * @param name column name, whose metadata type info should be returned
   * @return {@link ColumnMetadata} schema description of the column
   */
  ColumnMetadata getColumn(SchemaPath name);
}
