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
package org.apache.drill.exec.store.parquet;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.metastore.ColumnStatistic;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Holds common statistics about data in parquet group scan,
 * including information about total row count, columns counts, partition columns.
 */
public class ParquetGroupScanStatistics {

  // map from file names to maps of column name to partition value mappings
  private Map<String, Map<SchemaPath, Object>> partitionValueMap;
  // only for partition columns : value is unique for each partition
  private Map<SchemaPath, TypeProtos.MajorType> partitionColTypeMap;
  // total number of non-null value for each column in parquet files
  private Map<SchemaPath, MutableLong> columnValueCounts;
  // total number of rows (obtained from parquet footer)
  private long rowCount;


  public ParquetGroupScanStatistics(List<FileMetadata> rowGroupInfos) {
    collect(rowGroupInfos);
  }

  public ParquetGroupScanStatistics(ParquetGroupScanStatistics that) {
    this.partitionValueMap = new HashMap<>(that.partitionValueMap);
    this.partitionColTypeMap = new HashMap<>(that.partitionColTypeMap);
    this.columnValueCounts = new HashMap<>(that.columnValueCounts);
    this.rowCount = that.rowCount;
  }

  public long getColumnValueCount(SchemaPath column) {
    MutableLong count = columnValueCounts.get(column);
    return count != null ? count.getValue() : 0;
  }

  public List<SchemaPath> getPartitionColumns() {
    return new ArrayList<>(partitionColTypeMap.keySet());
  }

  public TypeProtos.MajorType getTypeForColumn(SchemaPath schemaPath) {
    return partitionColTypeMap.get(schemaPath);
  }

  public long getRowCount() {
    return rowCount;
  }

  public Object getPartitionValue(String path, SchemaPath column) {
    return partitionValueMap.get(path).get(column);
  }

  public void collect(List<FileMetadata> files) {
    resetHolders();
    boolean first = true;
    for (FileMetadata file : files) {
      Long rowCount = (Long) file.getStatistic(ColumnStatisticsKind.ROW_COUNT);
      for (Map.Entry<SchemaPath, ColumnStatistic> columnStatistic : file.getColumnStatistics().entrySet()) {
        SchemaPath schemaPath = columnStatistic.getKey();
        ColumnStatistic column = columnStatistic.getValue();
        MutableLong emptyCount = new MutableLong();
        MutableLong previousCount = columnValueCounts.putIfAbsent(schemaPath, emptyCount);
        if (previousCount == null) {
          previousCount = emptyCount;
        }
        Long nullsNum = (Long) column.getStatistic(ColumnStatisticsKind.NULLS_COUNT);
        if (previousCount.longValue() != GroupScan.NO_COLUMN_STATS && nullsNum != null && nullsNum != GroupScan.NO_COLUMN_STATS) {
          previousCount.add(rowCount - nullsNum);
        } else {
          previousCount.setValue(GroupScan.NO_COLUMN_STATS);
        }
        boolean partitionColumn = checkForPartitionColumn(column, first, rowCount, file.getFields().get(schemaPath), schemaPath);
        if (partitionColumn) {
          Map<SchemaPath, Object> map = partitionValueMap.computeIfAbsent(file.getLocation(), key -> new HashMap<>());
          Object value = map.get(schemaPath);
          Object currentValue = column.getStatistic(ColumnStatisticsKind.MAX_VALUE);
          if (value != null) {
            if (value != currentValue) {
              partitionColTypeMap.remove(schemaPath);
            }
          } else {
            // the value of a column with primitive type can not be null,
            // so checks that there are really null value and puts it to the map
            if (rowCount == column.getStatistic(ColumnStatisticsKind.NULLS_COUNT)) {
              map.put(schemaPath, null);
            } else {
              map.put(schemaPath, currentValue);
            }
          }
        } else {
          partitionColTypeMap.remove(schemaPath);
        }
      }
      this.rowCount += rowCount;
      first = false;
    }
  }

  /**
   * Re-init holders eigther during first instance creation or statistics update based on updated list of row groups.
   */
  private void resetHolders() {
    this.partitionValueMap = new HashMap<>();
    this.partitionColTypeMap = new HashMap<>();
    this.columnValueCounts = new HashMap<>();
    this.rowCount = 0;
  }

  /**
   * When reading the very first footer, any column is a potential partition column. So for the first footer, we check
   * every column to see if it is single valued, and if so, add it to the list of potential partition columns. For the
   * remaining footers, we will not find any new partition columns, but we may discover that what was previously a
   * potential partition column now no longer qualifies, so it needs to be removed from the list.
   *
   * @param columnMetadata column metadata
   * @param first if columns first appears in row group
   * @param rowCount row count
   *
   * @return whether column is a potential partition column
   */
  private boolean checkForPartitionColumn(ColumnStatistic columnMetadata,
                                          boolean first,
                                          Long rowCount,
                                          TypeProtos.MajorType type,
                                          SchemaPath schemaPath) {
    if (first) {
      if (hasSingleValue(columnMetadata, rowCount)) {
        partitionColTypeMap.put(schemaPath, type);
        return true;
      } else {
        return false;
      }
    } else {
      if (!partitionColTypeMap.keySet().contains(schemaPath)) {
        return false;
      } else {
        if (!hasSingleValue(columnMetadata, rowCount)) {
          partitionColTypeMap.remove(schemaPath);
          return false;
        }
        if (!type.equals(partitionColTypeMap.get(schemaPath))) {
          partitionColTypeMap.remove(schemaPath);
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks that the column chunk has a single value.
   * ColumnMetadata will have a non-null value iff the minValue and
   * the maxValue for the rowgroup are the same.
   *
   * @param columnChunkMetaData metadata to check
   * @param rowCount rows count in column chunk
   *
   * @return true if column has single value
   */
  private boolean hasSingleValue(ColumnStatistic columnChunkMetaData, long rowCount) {
    return columnChunkMetaData != null && isSingleVal(columnChunkMetaData, rowCount);
  }

  private boolean isSingleVal(ColumnStatistic columnChunkMetaData, long rowCount) {
    Long numNulls = (Long) columnChunkMetaData.getStatistic(ColumnStatisticsKind.NULLS_COUNT);
    if (numNulls != null && numNulls != GroupScan.NO_COLUMN_STATS) {
      Object min = columnChunkMetaData.getStatistic(ColumnStatisticsKind.MIN_VALUE);
      Object max = columnChunkMetaData.getStatistic(ColumnStatisticsKind.MAX_VALUE);
      if (min != null) {
        return (numNulls == 0 || numNulls == rowCount) && Objects.deepEquals(min, max);
      } else {
        return numNulls == rowCount && max == null;
      }
    }
    return false;
  }

}
