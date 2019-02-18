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

import org.apache.drill.exec.physical.base.GroupScan;

import java.util.List;

/**
 * Implementation of {@link CollectableColumnStatisticKind} which contain base
 * column statistic kinds with implemented {@code mergeStatistic()} method.
 */
public enum ColumnStatisticsKind implements CollectableColumnStatisticKind {

  /**
   * Column statistic kind which represents nulls count for the specific column.
   */
  NULLS_COUNT("nullsCount") {
    @Override
    public Object mergeStatistic(List<? extends ColumnStatistic> statistics) {
      long nullsCount = 0;
      for (ColumnStatistic statistic : statistics) {
        Long statNullsCount = (Long) statistic.getStatistic(this);
        if (statNullsCount == null || statNullsCount == GroupScan.NO_COLUMN_STATS) {
          return GroupScan.NO_COLUMN_STATS;
        } else {
          nullsCount += statNullsCount;
        }
      }
      return nullsCount;
    }
  },

  /**
   * Column statistic kind which represents min value of the specific column.
   */
  MIN_VALUE("minValue") {
    @Override
    @SuppressWarnings("unchecked")
    public Object mergeStatistic(List<? extends ColumnStatistic> statistics) {
      Object minValue = null;
      for (ColumnStatistic statistic : statistics) {
        Object statMinValue = statistic.getValueStatistic(this);
        if (statMinValue != null && (statistic.getValueComparator().compare(minValue, statMinValue) > 0 || minValue == null)) {
          minValue = statMinValue;
        }
      }
      return minValue;
    }

    @Override
    public boolean isValueStatistic() {
      return true;
    }
  },

  /**
   * Column statistic kind which represents max value of the specific column.
   */
  MAX_VALUE("maxValue") {
    @Override
    @SuppressWarnings("unchecked")
    public Object mergeStatistic(List<? extends ColumnStatistic> statistics) {
      Object maxValue = null;
      for (ColumnStatistic statistic : statistics) {
        Object statMaxValue = statistic.getValueStatistic(this);
        if (statMaxValue != null && statistic.getValueComparator().compare(maxValue, statMaxValue) < 0) {
          maxValue = statMaxValue;
        }
      }
      return maxValue;
    }

    @Override
    public boolean isValueStatistic() {
      return true;
    }
  };

  private final String statisticKey;

  ColumnStatisticsKind(String statisticKey) {
    this.statisticKey = statisticKey;
  }

  public String getName() {
    return statisticKey;
  }
}
