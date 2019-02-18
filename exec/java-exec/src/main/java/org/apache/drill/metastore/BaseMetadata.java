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
   * as Map of column {@code SchemaPath}s and corresponding {@code ColumnStatistic}s.
   *
   * @return statistics stored in current metadata
   */
  Map<SchemaPath, ColumnStatistic> getColumnStatistics();

  /**
   * Returns schema stored in current metadata represented as
   * {@link TupleSchema}.
   *
   * @return schema stored in current metadata
   */
  TupleSchema getSchema();

  /**
   * Returns value of non-column statistic which corresponds to specified {@link StatisticsKind}.
   *
   * @param statisticsKind statistic kind whose value should be returned
   * @return value of non-column statistic
   */
  Object getStatistic(StatisticsKind statisticsKind);

  /**
   * Returns value of column statistic which corresponds to specified {@link StatisticsKind}
   * for column with specified {@code columnName}.
   *
   * @param columnName     name of the column
   * @param statisticsKind statistic kind whose value should be returned
   * @return value of column statistic
   */
  Object getStatisticsForColumn(SchemaPath columnName, StatisticsKind statisticsKind);

  ColumnMetadata getColumn(SchemaPath name);
}
