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

import java.util.Comparator;

/**
 * Represents collection of statistic values for specific column.
 *
 * @param <T> type of column values
 */
public interface ColumnStatistic<T> {

  /**
   * Returns statistic value which corresponds to specified {@link StatisticsKind}.
   *
   * @param statisticsKind kind of statistic which value should be returned
   * @return statistic value
   */
  Object getStatistic(StatisticsKind statisticsKind);

  /**
   * Checks whether specified statistic kind is set in this column statistics.
   *
   * @param statisticsKind statistic kind to check
   * @return true if specified statistic kind is set
   */
  boolean containsStatistic(StatisticsKind statisticsKind);

  /**
   * Returns {@link Comparator} for comparing values with the same type as column values.
   *
   * @return {@link Comparator}
   */
  Comparator<T> getValueComparator();

  /**
   * Returns statistic associated with value type, like a min or max value etc.
   *
   * @param statisticsKind kind of statistic
   * @return statistic value for specified statistic kind
   */
  @SuppressWarnings("unchecked")
  default T getValueStatistic(StatisticsKind statisticsKind) {
    if (statisticsKind.isValueStatistic()) {
      return (T) getStatistic(statisticsKind);
    }
    return null;
  }
}
