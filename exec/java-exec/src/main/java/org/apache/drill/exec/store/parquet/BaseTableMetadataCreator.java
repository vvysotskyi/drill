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

import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.store.parquet.metadata.Metadata_V3;
import org.apache.drill.exec.store.parquet.stat.ParquetMetaStatCollector;
import org.apache.drill.metastore.ColumnStatistic;
import org.apache.drill.metastore.ColumnStatisticImpl;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.metastore.TableMetadata;
import org.apache.drill.shaded.guava.com.google.common.collect.HashBasedTable;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.shaded.guava.com.google.common.collect.Table;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base logic for creating ParquetTableMetadata taken from the ParquetGroupScan.
 */
public abstract class BaseTableMetadataCreator {

  private static Comparator<byte[]> UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR = Comparator.nullsFirst((b1, b2) ->
      PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(Binary.fromReusedByteArray(b1), Binary.fromReusedByteArray(b2)));

  private static Object NULL_VALUE = new Object();

  protected List<ReadEntryWithPath> entries;

  protected MetadataBase.ParquetTableMetadataBase parquetTableMetadata;
  protected Set<String> fileSet;
  protected final ParquetReaderConfig readerConfig;

  private List<SchemaPath> partitionColumns;
  protected String tableName;
  protected String tableLocation;

  public BaseTableMetadataCreator(List<ReadEntryWithPath> entries,
                          ParquetReaderConfig readerConfig,
                          Set<String> fileSet) {
    this.entries = entries;

    this.fileSet = fileSet;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
  }

  public BaseTableMetadataCreator(ParquetReaderConfig readerConfig) {
    this.entries = new ArrayList<>();
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
  }

  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  public TableMetadata getTableMetadata() {

    HashMap<String, Object> tableStatistics = new HashMap<>();
    tableStatistics.put(ColumnStatisticsKind.ROW_COUNT.getName(), getRowCount(parquetTableMetadata));

    HashSet<String> partitionKeys = new HashSet<>();

    LinkedHashMap<SchemaPath, TypeProtos.MajorType> fields = resolveFields(parquetTableMetadata);

    TupleSchema schema = new TupleSchema();
    for (Map.Entry<SchemaPath, TypeProtos.MajorType> pathTypePair : fields.entrySet()) {
      SchemaPathUtils.addColumnMetadata(pathTypePair.getKey(), schema, pathTypePair.getValue());
    }

    HashMap<SchemaPath, ColumnStatistic> columnStatistics = getColumnStatistics(parquetTableMetadata, fields.keySet());

    return new TableMetadata(tableName, tableLocation,
        schema, columnStatistics, tableStatistics, -1, "root", partitionKeys);
  }

  public List<PartitionMetadata> getPartitionMetadata() {
    Table<SchemaPath, Object, List<FileMetadata>> colValFile = HashBasedTable.create();
    ParquetGroupScanStatistics parquetGroupScanStatistics = new ParquetGroupScanStatistics(getFilesMetadata());
    List<FileMetadata> filesMetadata = getFilesMetadata();
    partitionColumns = parquetGroupScanStatistics.getPartitionColumns();
    for (FileMetadata fileMetadata : filesMetadata) {
      for (SchemaPath partitionColumn : partitionColumns) {
        Object partitionValue = parquetGroupScanStatistics.getPartitionValue(fileMetadata.getLocation(), partitionColumn);
        // Table cannot contain nulls
        partitionValue = partitionValue == null ? NULL_VALUE : partitionValue;
        List<FileMetadata> partitionFiles = colValFile.get(partitionColumn, partitionValue);
        if (partitionFiles == null) {
          partitionFiles = new ArrayList<>();
          colValFile.put(partitionColumn, partitionValue, partitionFiles);
        }
        partitionFiles.add(fileMetadata);
      }
    }

    List<PartitionMetadata> result = new ArrayList<>();

    for (SchemaPath logicalExpressions : colValFile.rowKeySet()) {
      for (List<FileMetadata> partValues : colValFile.row(logicalExpressions).values()) {
        result.add(getPartitionMetadata(logicalExpressions, partValues));
      }
    }

    return result;
  }

  protected abstract void initInternal() throws IOException;

  protected PartitionMetadata getPartitionMetadata(SchemaPath logicalExpressions, List<FileMetadata> files) {
    Map<SchemaPath, MutableLong> nullsCounts = new HashMap<>();
    Map<SchemaPath, Object> minVals = new HashMap<>();
    Map<SchemaPath, Object> maxVals = new HashMap<>();
    Map<SchemaPath, Comparator> valComparator = new HashMap<>();
    Set<String> locations = new HashSet<>();
    long partRowCount = 0;
    Set<SchemaPath> columns = new HashSet<>();

    for (FileMetadata file : files) {
      columns.addAll(file.getColumnStatistics().keySet());

      for (SchemaPath column : columns) {
        nullsCounts.computeIfAbsent(column, c -> new MutableLong());
      }
    }

    for (FileMetadata file : files) {
      locations.add(file.getLocation());
      Long fileRowCount = (Long) file.getStatistic(ColumnStatisticsKind.ROW_COUNT);
      if (fileRowCount != null && fileRowCount != GroupScan.NO_COLUMN_STATS) {
        partRowCount += fileRowCount;
      } else {
        fileRowCount = GroupScan.NO_COLUMN_STATS;
      }
      Set<SchemaPath> unhandledColumns = new HashSet<>(columns);
      for (SchemaPath colPath : file.getColumnStatistics().keySet()) {
        ColumnStatistic columnStatistic = file.getColumnStatistics().get(colPath);
        unhandledColumns.remove(colPath);

        MutableLong nullsCount = nullsCounts.get(colPath);

        if (nullsCount.longValue() != GroupScan.NO_COLUMN_STATS) {
          Long nulls = (Long) columnStatistic.getStatistic(ColumnStatisticsKind.NULLS_COUNT);
          if (nulls != null && GroupScan.NO_COLUMN_STATS != nulls) {
            nullsCount.add(nulls);
          } else {
            nullsCount.setValue(GroupScan.NO_COLUMN_STATS);
          }
        }

        Comparator comparator = columnStatistic.getValueComparator();
        valComparator.put(colPath, comparator);

        Object min = columnStatistic.getStatistic(ColumnStatisticsKind.MIN_VALUE);
        Object max = columnStatistic.getStatistic(ColumnStatisticsKind.MAX_VALUE);

        Object prevMin = minVals.get(colPath);
        if (min != null && (comparator.compare(prevMin, min) > 0 || prevMin == null)) {
          minVals.put(colPath, min);
        }
        if (max != null && comparator.compare(maxVals.get(colPath), max) < 0) {
          maxVals.put(colPath, max);
        }
      }
      // handle missed for file columns by collecting nulls count
      for (SchemaPath unhandledColumn : unhandledColumns) {
        MutableLong nullsCount = nullsCounts.get(unhandledColumn);
        if (nullsCount.longValue() != GroupScan.NO_COLUMN_STATS) {
          nullsCount.add(fileRowCount);
        }
      }
    }

    HashMap<SchemaPath, ColumnStatistic> columnStatistics = new HashMap<>();

    for (SchemaPath column : columns) {
      Map<String, Object> statistics = new HashMap<>();
      statistics.put(ColumnStatisticsKind.MIN_VALUE.getName(), minVals.get(column));
      statistics.put(ColumnStatisticsKind.MAX_VALUE.getName(), maxVals.get(column));
      statistics.put(ColumnStatisticsKind.NULLS_COUNT.getName(), nullsCounts.get(column).getValue());
      columnStatistics.put(column, new ColumnStatisticImpl(statistics, valComparator.get(column)));
    }

    HashMap<String, Object> partStatistics = new HashMap<>();
    partStatistics.put(ColumnStatisticsKind.ROW_COUNT.getName(), partRowCount);

    return
      new PartitionMetadata(logicalExpressions, files.iterator().next().getSchema(),
          columnStatistics, partStatistics, locations, tableName, -1);
  }

  public List<SchemaPath> getPartitionColumns() {
    return partitionColumns;
  }

  public List<FileMetadata> getFilesMetadata() {
    if (entries.isEmpty()) {
      return Collections.emptyList();
    }
    List<FileMetadata> result = new ArrayList<>();
    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      FileMetadata fileMetadata = getFileMetadata(file);
      result.add(fileMetadata);
    }

    return result;
  }

  protected FileMetadata getFileMetadata(MetadataBase.ParquetFileMetadata file) {
    LinkedHashMap<SchemaPath, TypeProtos.MajorType> columns = getFileFields(parquetTableMetadata, file);

    HashMap<String, Object> fileStatistics = new HashMap<>();
    fileStatistics.put(ColumnStatisticsKind.ROW_COUNT.getName(), getFileRowCount(file));

    TupleSchema schema = new TupleSchema();
    for (Map.Entry<SchemaPath, TypeProtos.MajorType> pathTypePair : columns.entrySet()) {
      SchemaPathUtils.addColumnMetadata(pathTypePair.getKey(), schema, pathTypePair.getValue());
    }

    return new FileMetadata(file.getPath(), schema,
        getFileColumnStatistics(file, columns.keySet()), fileStatistics, file.getPath(), tableName, -1);
  }

  public List<RowGroupMetadata> getRowGroupsMeta() {
    List<RowGroupMetadata> result = new ArrayList<>();

    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      int index = 0;
      FileMetadata fileMetadata = getFileMetadata(file);
      for (MetadataBase.RowGroupMetadata rowGroupMetadata : file.getRowGroups()) {

        HashMap<SchemaPath, ColumnStatistic> columnStatistics = getRowGroupColumnStatistics(rowGroupMetadata);
        HashMap<String, Object> rowGroupStatistics = new HashMap<>();
        rowGroupStatistics.put(ColumnStatisticsKind.ROW_COUNT.getName(), rowGroupMetadata.getRowCount());
        rowGroupStatistics.put("start", rowGroupMetadata.getStart());
        rowGroupStatistics.put("length", rowGroupMetadata.getLength());

        LinkedHashMap<SchemaPath, TypeProtos.MajorType> columns = getRowGroupFields(parquetTableMetadata, rowGroupMetadata);

        TupleSchema schema = new TupleSchema();
        for (Map.Entry<SchemaPath, TypeProtos.MajorType> pathTypePair : columns.entrySet()) {
          SchemaPathUtils.addColumnMetadata(pathTypePair.getKey(), schema, pathTypePair.getValue());
        }

        RowGroupMetadata groupMetadata = new RowGroupMetadata(
            schema, columnStatistics, rowGroupStatistics, rowGroupMetadata.getHostAffinity(), index++, fileMetadata.getLocation());
        result.add(groupMetadata);
      }
    }

    return result;
  }

  private long getRowCount(MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    long sum = 0L;
    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      long rowCount = getFileRowCount(file);
      if (rowCount == GroupScan.NO_COLUMN_STATS) {
        return GroupScan.NO_COLUMN_STATS;
      }
      sum += rowCount;
    }
    return sum;
  }

  private long getFileRowCount(MetadataBase.ParquetFileMetadata file) {
    long sum = 0L;
    for (MetadataBase.RowGroupMetadata rowGroupMetadata : file.getRowGroups()) {
      Long rowCount = rowGroupMetadata.getRowCount();
      if (rowCount == null || rowCount == GroupScan.NO_COLUMN_STATS) {
        return GroupScan.NO_COLUMN_STATS;
      }
      sum += rowCount;
    }
    return sum;
  }

  @SuppressWarnings("unchecked")
  private static HashMap<SchemaPath, ColumnStatistic> getColumnStatistics(
      MetadataBase.ParquetTableMetadataBase parquetTableMetadata, Set<SchemaPath> columns) {
    Map<SchemaPath, MutableLong> nullsCounts = new HashMap<>();
    Map<SchemaPath, Object> minVals = new HashMap<>();
    Map<SchemaPath, Object> maxVals = new HashMap<>();
    Map<SchemaPath, Comparator> valComparator = new HashMap<>();
    for (SchemaPath column : columns) {
      nullsCounts.put(column, new MutableLong());
    }

    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      for (MetadataBase.RowGroupMetadata rowGroupMetadata : file.getRowGroups()) {
        Set<SchemaPath> unhandledColumns = new HashSet<>(columns);
        for (MetadataBase.ColumnMetadata column : rowGroupMetadata.getColumns()) {
          collectSingleMetadataEntry(parquetTableMetadata, nullsCounts, minVals, maxVals, valComparator, column);
          unhandledColumns.remove(SchemaPath.getCompoundPath(column.getName()));
        }
        // handle missed for file columns by collecting nulls count
        for (SchemaPath unhandledColumn : unhandledColumns) {
          MutableLong nullsCount = nullsCounts.get(unhandledColumn);
          if (nullsCount.longValue() != GroupScan.NO_COLUMN_STATS) {
            nullsCount.add(rowGroupMetadata.getRowCount());
          }
        }
      }
    }
    HashMap<SchemaPath, ColumnStatistic> columnStatistics = new HashMap<>();
    for (SchemaPath column : columns) {
      Map<String, Object> statistics = new HashMap<>();
      statistics.put(ColumnStatisticsKind.MIN_VALUE.getName(), minVals.get(column));
      statistics.put(ColumnStatisticsKind.MAX_VALUE.getName(), maxVals.get(column));
      statistics.put(ColumnStatisticsKind.NULLS_COUNT.getName(), nullsCounts.get(column).getValue());
      columnStatistics.put(column, new ColumnStatisticImpl(statistics, valComparator.get(column)));
    }

    return columnStatistics;
  }

  private static void collectSingleMetadataEntry(MetadataBase.ParquetTableMetadataBase parquetTableMetadata,
      Map<SchemaPath, MutableLong> nullsCounts, Map<SchemaPath, Object> minVals, Map<SchemaPath, Object> maxVals,
      Map<SchemaPath, Comparator> valComparator, MetadataBase.ColumnMetadata column) {
    PrimitiveType.PrimitiveTypeName primitiveType = getPrimitiveTypeName(parquetTableMetadata, column);
    OriginalType originalType = getOriginalType(parquetTableMetadata, column);
    SchemaPath colPath = SchemaPath.getCompoundPath(column.getName());
    MutableLong nullsCount = nullsCounts.get(colPath);
    if (nullsCount.longValue() != GroupScan.NO_COLUMN_STATS) {
      Long nulls = column.getNulls();
      if (column.isNumNullsSet() && nulls != null) {
        nullsCount.add(nulls);
      } else {
        nullsCount.setValue(GroupScan.NO_COLUMN_STATS);
      }
    }
    Comparator comparator = valComparator.get(colPath);

    Pair<Object, Object> minMax = getMinMax(column, primitiveType, originalType);

    Object min = minMax.getKey();
    Object max = minMax.getValue();

    if (comparator == null) {
      comparator = getComparator(primitiveType, originalType);

      valComparator.put(colPath, comparator);

      minVals.put(colPath, min);
      maxVals.put(colPath, max);
    } else {
      Object prevMin = minVals.get(colPath);
      if (min != null && (comparator.compare(prevMin, min) > 0 || prevMin == null)) {
        minVals.put(colPath, min);
      }
      if (max != null && comparator.compare(maxVals.get(colPath), max) < 0) {
        maxVals.put(colPath, max);
      }
    }
  }

  private static Pair<Object, Object> getMinMax(MetadataBase.ColumnMetadata column, PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType) {
    Object minValue = column.getMinValue();
    Object maxValue = column.getMaxValue();
    switch (primitiveType) {
      case BOOLEAN:
        if (minValue != null) {
          minValue = Boolean.parseBoolean(minValue.toString());
        }
        if (maxValue != null) {
          maxValue = Boolean.parseBoolean(maxValue.toString());
        }
        break;

      case INT32:
        if (originalType != null) {
          switch (originalType) {
            case DATE:
              if (minValue != null) {
                minValue = ParquetMetaStatCollector.convertToDrillDateValue(Integer.parseInt(minValue.toString()));
              }
              if (maxValue != null) {
                maxValue = ParquetMetaStatCollector.convertToDrillDateValue(Integer.parseInt(maxValue.toString()));
              }
              break;
            case DECIMAL:
              if (minValue != null) {
                minValue = new BigDecimal(minValue.toString()).unscaledValue();
              }
              if (maxValue != null) {
                maxValue = new BigDecimal(maxValue.toString()).unscaledValue();
              }
              break;
            default:
              if (minValue != null) {
                minValue = Integer.parseInt(minValue.toString());
              }
              if (maxValue != null) {
                maxValue = Integer.parseInt(maxValue.toString());
              }
          }
        } else {
          if (minValue != null) {
            minValue = Integer.parseInt(minValue.toString());
          }
          if (maxValue != null) {
            maxValue = Integer.parseInt(maxValue.toString());
          }
        }
        break;

      case INT64:
        if (originalType == OriginalType.DECIMAL) {
          if (minValue != null) {
            minValue = new BigDecimal(minValue.toString()).unscaledValue();
          }
          if (maxValue != null) {
            maxValue = new BigDecimal(maxValue.toString()).unscaledValue();
          }
        } else {
          if (minValue != null) {
            minValue = Long.parseLong(minValue.toString());
          }
          if (maxValue != null) {
            maxValue = Long.parseLong(maxValue.toString());
          }
        }
        break;

      case FLOAT:
        if (minValue != null) {
          minValue = Float.parseFloat(minValue.toString());
        }
        if (maxValue != null) {
          maxValue = Float.parseFloat(maxValue.toString());
        }
        break;

      case DOUBLE:
        if (minValue != null) {
          minValue = Double.parseDouble(minValue.toString());
        }
        if (maxValue != null) {
          maxValue = Double.parseDouble(maxValue.toString());
        }
        break;

      case INT96:
        if (minValue instanceof Binary) {
          minValue = ((Binary) minValue).getBytes();
        }
        if (maxValue instanceof Binary) {
          maxValue = ((Binary) maxValue).getBytes();
        }
        break;

      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        if (originalType == OriginalType.DECIMAL) {
          if (minValue instanceof Binary) {
            minValue = new BigInteger(((Binary) minValue).getBytes());
          } else if (minValue instanceof byte[]) {
            minValue = new BigInteger((byte[]) minValue);
          }
          if (maxValue instanceof Binary) {
            maxValue = new BigInteger(((Binary) maxValue).getBytes());
          } else if (maxValue instanceof byte[]) {
            maxValue = new BigInteger((byte[]) maxValue);
          }
        } else if (originalType == OriginalType.INTERVAL) {
          if (minValue instanceof Binary) {
            minValue = ((Binary) minValue).getBytes();
          }
          if (maxValue instanceof Binary) {
            maxValue = ((Binary) maxValue).getBytes();
          }
        } else {
          if (minValue instanceof Binary) {
            minValue = new String(((Binary) minValue).getBytes());
          } else if (minValue instanceof byte[]) {
            minValue = new String((byte[]) minValue);
          }
          if (maxValue instanceof Binary) {
            maxValue = new String(((Binary) maxValue).getBytes());
          } else if (maxValue instanceof byte[]) {
            maxValue = new String((byte[]) maxValue);
          }
        }
    }
    return Pair.of(minValue, maxValue);
  }

  private static Comparator getComparator(PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType) {
    if (originalType != null) {
      switch (originalType) {
        case UINT_8:
        case UINT_16:
        case UINT_32:
          return Comparator.nullsFirst(Integer::compareUnsigned);
        case UINT_64:
          return Comparator.nullsFirst(Long::compareUnsigned);
        case DATE:
        case INT_8:
        case INT_16:
        case INT_32:
        case INT_64:
        case TIME_MICROS:
        case TIME_MILLIS:
        case TIMESTAMP_MICROS:
        case TIMESTAMP_MILLIS:
        case DECIMAL:
        case UTF8:
          return Comparator.nullsFirst(Comparator.naturalOrder());
        case INTERVAL:
          return UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
        default:
          return Comparator.nullsFirst(Comparator.naturalOrder());
      }
    } else {
      switch (primitiveType) {
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return Comparator.nullsFirst(Comparator.naturalOrder());
        case INT96:
          return UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitiveType);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private HashMap<SchemaPath, ColumnStatistic> getFileColumnStatistics(
      MetadataBase.ParquetFileMetadata file, Set<SchemaPath> columns) {
    Map<SchemaPath, MutableLong> nullsCounts = new HashMap<>();
    Map<SchemaPath, Object> minVals = new HashMap<>();
    Map<SchemaPath, Object> maxVals = new HashMap<>();
    Map<SchemaPath, Comparator> valComparator = new HashMap<>();
    for (SchemaPath column : columns) {
      nullsCounts.put(column, new MutableLong());
    }

    for (MetadataBase.RowGroupMetadata rowGroupMetadata : file.getRowGroups()) {
      for (MetadataBase.ColumnMetadata column : rowGroupMetadata.getColumns()) {
        collectSingleMetadataEntry(parquetTableMetadata, nullsCounts, minVals, maxVals, valComparator, column);
      }
    }
    HashMap<SchemaPath, ColumnStatistic> columnStatistics = new HashMap<>();
    for (SchemaPath column : columns) {
      Map<String, Object> statistics = new HashMap<>();
      statistics.put(ColumnStatisticsKind.MIN_VALUE.getName(), minVals.get(column));
      statistics.put(ColumnStatisticsKind.MAX_VALUE.getName(), maxVals.get(column));
      statistics.put(ColumnStatisticsKind.NULLS_COUNT.getName(), nullsCounts.get(column).getValue());
      columnStatistics.put(column, new ColumnStatisticImpl(statistics, valComparator.get(column)));
    }

    return columnStatistics;
  }

  @SuppressWarnings("unchecked")
  private HashMap<SchemaPath, ColumnStatistic> getRowGroupColumnStatistics(MetadataBase.RowGroupMetadata rowGroupMetadata) {

    HashMap<SchemaPath, ColumnStatistic> columnStatistics = new HashMap<>();

    for (MetadataBase.ColumnMetadata column : rowGroupMetadata.getColumns()) {
      SchemaPath colPath = SchemaPath.getCompoundPath(column.getName());

      Long nulls = column.getNulls();
      if (!column.isNumNullsSet() || nulls == null) {
        nulls = GroupScan.NO_COLUMN_STATS;
      }
      PrimitiveType.PrimitiveTypeName primitiveType = getPrimitiveTypeName(parquetTableMetadata, column);
      OriginalType originalType = getOriginalType(parquetTableMetadata, column);
      Comparator comparator = getComparator(primitiveType, originalType);

      Pair<Object, Object> minMaxPair = getMinMax(column, primitiveType, originalType);

      Map<String, Object> statistics = new HashMap<>();
      statistics.put(ColumnStatisticsKind.MIN_VALUE.getName(), minMaxPair.getKey());
      statistics.put(ColumnStatisticsKind.MAX_VALUE.getName(), minMaxPair.getValue());
      statistics.put(ColumnStatisticsKind.NULLS_COUNT.getName(), nulls);
      columnStatistics.put(colPath, new ColumnStatisticImpl(statistics, comparator));
    }

    return columnStatistics;
  }

  private static LinkedHashMap<SchemaPath, TypeProtos.MajorType> resolveFields(MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    LinkedHashMap<SchemaPath, TypeProtos.MajorType> columns = new LinkedHashMap<>();
    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      // row groups in the file have the same schema, so using the first one
      LinkedHashMap<SchemaPath, TypeProtos.MajorType> fileColumns = getFileFields(parquetTableMetadata, file);
      for (Map.Entry<SchemaPath, TypeProtos.MajorType> schemaPathMajorTypeEntry : fileColumns.entrySet()) {

        SchemaPath columnPath = schemaPathMajorTypeEntry.getKey();
        TypeProtos.MajorType majorType = columns.get(columnPath);
        if (majorType == null) {
          columns.put(columnPath, schemaPathMajorTypeEntry.getValue());
        } else {
          TypeProtos.MinorType leastRestrictiveType = TypeCastRules.getLeastRestrictiveType(Arrays.asList(majorType.getMinorType(), schemaPathMajorTypeEntry.getValue().getMinorType()));
          if (leastRestrictiveType != majorType.getMinorType()) {
            columns.put(columnPath, schemaPathMajorTypeEntry.getValue());
          }
        }
      }
    }
    return columns;
  }

  private static LinkedHashMap<SchemaPath, TypeProtos.MajorType> getFileFields(
      MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ParquetFileMetadata file) {

    // does not resolve types considering all row groups, just takes type from the first row group.
    return getRowGroupFields(parquetTableMetadata, file.getRowGroups().iterator().next());
  }

  private static LinkedHashMap<SchemaPath, TypeProtos.MajorType> getRowGroupFields(
    MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.RowGroupMetadata rowGroup) {
    LinkedHashMap<SchemaPath, TypeProtos.MajorType> columns = new LinkedHashMap<>();
    for (MetadataBase.ColumnMetadata column : rowGroup.getColumns()) {

      PrimitiveType.PrimitiveTypeName primitiveType = getPrimitiveTypeName(parquetTableMetadata, column);
      OriginalType originalType = getOriginalType(parquetTableMetadata, column);
      int precision = 0;
      int scale = 0;
      int definitionLevel = 1;
      int repetitionLevel = 0;
      // only ColumnTypeMetadata_v3 stores information about scale, precision, repetition level and definition level
      if (parquetTableMetadata.hasColumnMetadata() && parquetTableMetadata instanceof Metadata_V3.ParquetTableMetadata_v3) {
        Metadata_V3.ColumnTypeMetadata_v3 columnTypeInfo =
          ((Metadata_V3.ParquetTableMetadata_v3) parquetTableMetadata).getColumnTypeInfo(column.getName());
        scale = columnTypeInfo.scale;
        precision = columnTypeInfo.precision;
        repetitionLevel = parquetTableMetadata.getRepetitionLevel(column.getName());
        definitionLevel = parquetTableMetadata.getDefinitionLevel(column.getName());
      }
      TypeProtos.DataMode mode;
      if (repetitionLevel >= 1) {
        mode = TypeProtos.DataMode.REPEATED;
      } else if (repetitionLevel == 0 && definitionLevel == 0) {
        mode = TypeProtos.DataMode.REQUIRED;
      } else {
        mode = TypeProtos.DataMode.OPTIONAL;
      }
      TypeProtos.MajorType columnType =
          TypeProtos.MajorType.newBuilder(ParquetReaderUtility.getType(primitiveType, originalType, scale, precision))
              .setMode(mode)
              .build();

      SchemaPath columnPath = SchemaPath.getCompoundPath(column.getName());
      TypeProtos.MajorType majorType = columns.get(columnPath);
      if (majorType == null) {
        columns.put(columnPath, columnType);
      } else {
        TypeProtos.MinorType leastRestrictiveType = TypeCastRules.getLeastRestrictiveType(Arrays.asList(majorType.getMinorType(), columnType.getMinorType()));
        if (leastRestrictiveType != majorType.getMinorType()) {
          columns.put(columnPath, columnType);
        }
      }
    }
    return columns;
  }

  private static OriginalType getOriginalType(MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ColumnMetadata column) {
    OriginalType originalType = parquetTableMetadata.getOriginalType(column.getName());
    // for the case of parquet metadata v1 version, type information isn't stored in parquetTableMetadata, but in ColumnMetadata
    if (originalType == null) {
      originalType = column.getOriginalType();
    }
    return originalType;
  }

  private static PrimitiveType.PrimitiveTypeName getPrimitiveTypeName(MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ColumnMetadata column) {
    PrimitiveType.PrimitiveTypeName primitiveType = parquetTableMetadata.getPrimitiveType(column.getName());
    // for the case of parquet metadata v1 version, type information isn't stored in parquetTableMetadata, but in ColumnMetadata
    if (primitiveType == null) {
      primitiveType = column.getPrimitiveType();
    }
    return primitiveType;
  }

  protected void init() throws IOException {
    initInternal();

    assert parquetTableMetadata != null;

    if (fileSet == null) {
      fileSet = new HashSet<>();
      fileSet.addAll(parquetTableMetadata.getFiles().stream()
        .map(MetadataBase.ParquetFileMetadata::getPath)
        .collect(Collectors.toSet()));
    }
  }
  // overridden protected methods block end
}
