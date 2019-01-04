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

public interface ColumnStatistic<T> {
  Object getStatistic(StatisticsKind statisticsKind);
  boolean containsStatistic(StatisticsKind statisticsKind);
  Comparator<T> getValueComparator();

  /**
   * Returns statistic associated with value type, like a min or max value etc.
   *
   * @param statisticsKind kind of statistic
   * @return statistic value for specified statistic kind
   */
  @SuppressWarnings("unchecked")
  default T getValueStatistic(StatisticsKind statisticsKind) {
    if (statisticsKind.valueStatistic()) {
      return (T) getStatistic(statisticsKind);
    }
    return null;
  }
}
