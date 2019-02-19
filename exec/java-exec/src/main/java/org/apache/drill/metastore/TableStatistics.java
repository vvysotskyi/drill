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
import org.apache.drill.metastore.expr.StatisticName;

import java.util.List;

/**
 * Implementation of {@link CollectableColumnStatisticKind} which contain base
 * table statistic kinds with implemented {@code mergeStatistic()} method.
 */
public enum TableStatistics implements CollectableTableStatisticKind {
  /**
   * Table statistic kind which represents row count for the specific column.
   */
  ROW_COUNT(StatisticName.ROW_COUNT) {
    @Override
    public Long mergeStatistic(List<? extends BaseMetadata> statistics) {
      long rowCount = 0;
      for (BaseMetadata statistic : statistics) {
        Long statRowCount = getValue(statistic);
        if (statRowCount == null || statRowCount == GroupScan.NO_COLUMN_STATS) {
          rowCount = GroupScan.NO_COLUMN_STATS;
          break;
        } else {
          rowCount += statRowCount;
        }
      }
      return rowCount;
    }

    @Override
    public Long getValue(BaseMetadata metadata) {
      Long rowCount = (Long) metadata.getStatistic(this);
      return rowCount != null ? rowCount : GroupScan.NO_COLUMN_STATS;
    }
  };

  private final String statisticKey;

  TableStatistics(String statisticKey) {
    this.statisticKey = statisticKey;
  }

  public String getName() {
    return statisticKey;
  }

  /**
   * Returns value which corresponds to this statistic kind,
   * obtained from specified {@link BaseMetadata}.
   *
   * @param metadata the source of statistic value
   * @return value which corresponds to this statistic kind
   */
  public abstract Object getValue(BaseMetadata metadata);
}