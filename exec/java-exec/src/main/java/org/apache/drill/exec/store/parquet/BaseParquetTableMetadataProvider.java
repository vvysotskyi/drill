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

import org.apache.commons.lang3.tuple.Pair;

import org.apache.drill.exec.physical.base.ParquetTableMetadataProvider;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.CollectableColumnStatisticKind;
import org.apache.drill.metastore.TableStatistics;
import org.apache.drill.shaded.guava.com.google.common.collect.HashBasedTable;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimaps;
import org.apache.drill.shaded.guava.com.google.common.collect.SetMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Table;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.exec.store.parquet.metadata.Metadata_V3;
import org.apache.drill.exec.store.parquet.stat.ParquetMetaStatCollector;
import org.apache.drill.metastore.ColumnStatistic;
import org.apache.drill.metastore.ColumnStatisticImpl;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.metastore.TableMetadata;
import org.apache.drill.shaded.guava.com.google.common.primitives.Longs;
import org.apache.drill.shaded.guava.com.google.common.primitives.UnsignedBytes;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseParquetTableMetadataProvider implements ParquetTableMetadataProvider {

  private static final Comparator<byte[]> UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR = Comparator.nullsFirst((b1, b2) ->
      PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(Binary.fromReusedByteArray(b1), Binary.fromReusedByteArray(b2)));

  private static final List<CollectableColumnStatisticKind> PARQUET_STATISTICS =
      ImmutableList.of(
          ColumnStatisticsKind.MAX_VALUE,
          ColumnStatisticsKind.MIN_VALUE,
          ColumnStatisticsKind.NULLS_COUNT);

  static final Object NULL_VALUE = new Object();

  private ParquetGroupScanStatistics<? extends BaseMetadata> parquetGroupScanStatistics;

  protected List<ReadEntryWithPath> entries;

  protected MetadataBase.ParquetTableMetadataBase parquetTableMetadata;
  protected Set<String> fileSet;
  protected /*final*/ ParquetReaderConfig readerConfig;

  private List<SchemaPath> partitionColumns;
  protected String tableName;
  protected String tableLocation;

  private List<RowGroupMetadata> rowGroups;
  private TableMetadata tableMetadata;
  private List<PartitionMetadata> partitions; // replace with Map, to obtain PartitionMetadata with a partitionColumnName
  private List<FileMetadata> files;
  protected boolean usedMetadataCache; // false by default

  private boolean collectMetadata = true;

  public BaseParquetTableMetadataProvider(List<ReadEntryWithPath> entries,
                                  ParquetReaderConfig readerConfig,
                                  Set<String> fileSet) {
    this(readerConfig, entries);
    this.fileSet = fileSet;
  }

  public BaseParquetTableMetadataProvider(ParquetReaderConfig readerConfig, List<ReadEntryWithPath> entries) {
    this.entries = entries == null ? new ArrayList<>() : entries;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
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

  @Override
  @SuppressWarnings("unchecked")
  public TableMetadata getTableMetadata() {
    if (tableMetadata == null) {

      HashMap<String, Object> tableStatistics = new HashMap<>();

      HashSet<String> partitionKeys = new HashSet<>();

      LinkedHashMap<SchemaPath, TypeProtos.MajorType> fields = resolveFields(parquetTableMetadata);

      TupleSchema schema = new TupleSchema();
      for (Map.Entry<SchemaPath, TypeProtos.MajorType> pathTypePair : fields.entrySet()) {
        SchemaPathUtils.addColumnMetadata(pathTypePair.getKey(), schema, pathTypePair.getValue());
      }

      Map<SchemaPath, ColumnStatistic> columnStatistics;
      if (collectMetadata) {
        List<? extends BaseMetadata> metadata = getFilesMetadata();
        if (metadata == null || metadata.isEmpty()) {
          metadata = getRowGroupsMeta();
        }
        tableStatistics.put(TableStatistics.ROW_COUNT.getName(), TableStatistics.ROW_COUNT.mergeStatistic(metadata));
        columnStatistics = getColumnStatistics(metadata, fields.keySet(), PARQUET_STATISTICS);
      } else {
        columnStatistics = new HashMap<>();
        tableStatistics.put(TableStatistics.ROW_COUNT.getName(), getParquetGroupScanStatistics().getRowCount());

        for (SchemaPath partitionColumn : fields.keySet()) {
          long columnValueCount = getParquetGroupScanStatistics().getColumnValueCount(partitionColumn);
          ImmutableMap<String, Long> stats = ImmutableMap.of(
              TableStatistics.ROW_COUNT.getName(), columnValueCount,
              ColumnStatisticsKind.NULLS_COUNT.getName(), getParquetGroupScanStatistics().getRowCount() - columnValueCount);
          columnStatistics.put(partitionColumn, new ColumnStatisticImpl(stats, getNaturalNullsFirstComparator()));
        }
      }
      tableMetadata = new TableMetadata(tableName, tableLocation, schema, columnStatistics, tableStatistics,
          -1, "root", partitionKeys);
    }

    return tableMetadata;
  }

  private ParquetGroupScanStatistics<? extends BaseMetadata> getParquetGroupScanStatistics() {
    if (parquetGroupScanStatistics == null) {
      if (collectMetadata) {
        parquetGroupScanStatistics = new ParquetGroupScanStatistics<>(getFilesMetadata());
      } else {
        parquetGroupScanStatistics = new ParquetGroupScanStatistics<>(getRowGroupsMeta());
      }
    }
    return parquetGroupScanStatistics;
  }

  @Override
  public List<SchemaPath> getPartitionColumns() {
    if (partitionColumns == null) {
      partitionColumns = getParquetGroupScanStatistics().getPartitionColumns();
    }
    return partitionColumns;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PartitionMetadata> getPartitionsMetadata() {
    if (partitions == null) {
      partitions = new ArrayList<>();
      if (collectMetadata) {
        Table<SchemaPath, Object, List<FileMetadata>> colValFile = HashBasedTable.create();

        List<FileMetadata> filesMetadata = getFilesMetadata();
        partitionColumns = getParquetGroupScanStatistics().getPartitionColumns();
        for (FileMetadata fileMetadata : filesMetadata) {
          for (SchemaPath partitionColumn : partitionColumns) {
            Object partitionValue = getParquetGroupScanStatistics().getPartitionValue(fileMetadata.getLocation(), partitionColumn);
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

        for (SchemaPath logicalExpressions : colValFile.rowKeySet()) {
          for (List<FileMetadata> partValues : colValFile.row(logicalExpressions).values()) {
            partitions.add(getPartitionMetadata(logicalExpressions, partValues));
          }
        }
      } else {
        for (SchemaPath partitionColumn : getParquetGroupScanStatistics().getPartitionColumns()) {
          Map<String, Object> partitionPaths = getParquetGroupScanStatistics().getPartitionPaths(partitionColumn);
          SetMultimap<Object, String> partitionsForValue = Multimaps.newSetMultimap(new HashMap<>(), HashSet::new);

          for (Map.Entry<String, Object> stringObjectEntry : partitionPaths.entrySet()) {
            partitionsForValue.put(stringObjectEntry.getValue(), stringObjectEntry.getKey());
          }

          for (Map.Entry<Object, Collection<String>> valueLocationsEntry : partitionsForValue.asMap().entrySet()) {
            HashMap<SchemaPath, ColumnStatistic> columnStatistics = new HashMap<>();

            Map<String, Object> statistics = new HashMap<>();
            Object partitionKey = valueLocationsEntry.getKey();
            partitionKey = valueLocationsEntry.getKey() == NULL_VALUE ? null : partitionKey;
            statistics.put(ColumnStatisticsKind.MIN_VALUE.getName(), partitionKey);
            statistics.put(ColumnStatisticsKind.MAX_VALUE.getName(), partitionKey);
            // incorrect row count, but it is ok, since nulls count is set here.
            statistics.put(ColumnStatisticsKind.NULLS_COUNT.getName(), partitionKey != null ? 0 : getParquetGroupScanStatistics().getRowCount());
            statistics.put(TableStatistics.ROW_COUNT.getName(), getParquetGroupScanStatistics().getRowCount());
            columnStatistics.put(partitionColumn,
                new ColumnStatisticImpl<>(statistics,
                    getComparator(getParquetGroupScanStatistics().getTypeForColumn(partitionColumn).getMinorType())));

            partitions.add(new PartitionMetadata(partitionColumn, getTableMetadata().getSchema(),
                columnStatistics, statistics, (Set<String>) valueLocationsEntry.getValue(), tableName, -1));
          }
        }
      }
    }
    return partitions;
  }

  @Override
  public PartitionMetadata getPartitionMetadata(SchemaPath columnName) {
    return getPartitionsMetadata().stream()
        .filter(Objects::nonNull)
        .filter(partitionMetadata -> partitionMetadata.getColumn().equals(columnName))
        .findFirst()
        .orElse(null);
  }

  private PartitionMetadata getPartitionMetadata(SchemaPath logicalExpressions, List<FileMetadata> files) {
    Set<String> locations = new HashSet<>();
    Set<SchemaPath> columns = new HashSet<>();

    for (FileMetadata file : files) {
      columns.addAll(file.getColumnStatistics().keySet());
      locations.add(file.getLocation());
    }

    HashMap<String, Object> partStatistics = new HashMap<>();
    partStatistics.put(TableStatistics.ROW_COUNT.getName(), TableStatistics.ROW_COUNT.mergeStatistic(files));

    return new PartitionMetadata(logicalExpressions,
        files.iterator().next().getSchema(), getColumnStatistics(files, columns, PARQUET_STATISTICS),
        partStatistics, locations, tableName, -1);
  }

  @Override
  public FileMetadata getFileMetadata() {
    throw new UnsupportedOperationException("This mechanism should be implemented");
  }

  @Override
  public List<FileMetadata> getFilesForPartition(PartitionMetadata partition) {
    return null;
  }

  @Override
  public List<FileMetadata> getFilesMetadata() {
    if (files == null) {
      if (entries.isEmpty() || !collectMetadata) {
        return Collections.emptyList();
      }
      boolean addRowGroups = false;
      files = new ArrayList<>();
      if (rowGroups == null) {
        rowGroups = new ArrayList<>();
        addRowGroups = true;
      }
      for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        int index = 0;
        List<RowGroupMetadata> fileRowGroups = new ArrayList<>();
        for (MetadataBase.RowGroupMetadata rowGroup : file.getRowGroups()) {
          RowGroupMetadata rowGroupMetadata = getRowGroupMetadata(rowGroup, index++, file.getPath());
          fileRowGroups.add(rowGroupMetadata);

          if (addRowGroups) {
            rowGroups.add(rowGroupMetadata);
          }
        }

        FileMetadata fileMetadata = getFileMetadata(fileRowGroups);
        files.add(fileMetadata);
      }
    }
    return files;
  }

  private FileMetadata getFileMetadata(List<RowGroupMetadata> rowGroups) {
    if (rowGroups.isEmpty()) {
      return null;
    }
    HashMap<String, Object> fileStatistics = new HashMap<>();
    fileStatistics.put(TableStatistics.ROW_COUNT.getName(), TableStatistics.ROW_COUNT.mergeStatistic(rowGroups));

    TupleSchema schema = rowGroups.iterator().next().getSchema();

    return new FileMetadata(rowGroups.iterator().next().getLocation(), schema,
        getColumnStatistics(rowGroups, rowGroups.iterator().next().getColumnStatistics().keySet(), PARQUET_STATISTICS),
        fileStatistics, tableName, -1);
  }

  @Override
  public Set<String> getFileSet() {
    return fileSet;
  }

  @Override
  public boolean isUsedMetadataCache() {
    return usedMetadataCache;
  }

  @Override
  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  @Override
  public List<RowGroupMetadata> getRowGroupsMeta() {
    if (rowGroups == null) {
      rowGroups = new ArrayList<>();
      for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        int index = 0;
        for (MetadataBase.RowGroupMetadata rowGroupMetadata : file.getRowGroups()) {
          rowGroups.add(getRowGroupMetadata(rowGroupMetadata, index++, file.getPath()));
        }
      }
    }
    return rowGroups;
  }

  private RowGroupMetadata getRowGroupMetadata(MetadataBase.RowGroupMetadata rowGroupMetadata, int rgIndexInFile, String location) {
    HashMap<SchemaPath, ColumnStatistic> columnStatistics = getRowGroupColumnStatistics(rowGroupMetadata);
    HashMap<String, Object> rowGroupStatistics = new HashMap<>();
    rowGroupStatistics.put(TableStatistics.ROW_COUNT.getName(), rowGroupMetadata.getRowCount());
    rowGroupStatistics.put("start", rowGroupMetadata.getStart());
    rowGroupStatistics.put("length", rowGroupMetadata.getLength());

    LinkedHashMap<SchemaPath, TypeProtos.MajorType> columns = getRowGroupFields(parquetTableMetadata, rowGroupMetadata);

    TupleSchema schema = new TupleSchema();
    for (Map.Entry<SchemaPath, TypeProtos.MajorType> pathTypePair : columns.entrySet()) {
      SchemaPathUtils.addColumnMetadata(pathTypePair.getKey(), schema, pathTypePair.getValue());
    }

    return new RowGroupMetadata(
      schema, columnStatistics, rowGroupStatistics, rowGroupMetadata.getHostAffinity(), rgIndexInFile, location);
  }

  protected abstract void initInternal() throws IOException;

  @SuppressWarnings("unchecked")
  public static <T extends BaseMetadata> Map<SchemaPath, ColumnStatistic> getColumnStatistics(
      List<T> rowGroups, Set<SchemaPath> columns, List<CollectableColumnStatisticKind> statisticsToCollect) {
    HashMap<SchemaPath, ColumnStatistic> columnStatistics = new HashMap<>();

    for (SchemaPath column : columns) {
      List<ColumnStatistic> statisticsList = new ArrayList<>();
      for (T metadata : rowGroups) {
        ColumnStatistic columnStatistic = metadata.getColumnStatistics().get(column);
        if (columnStatistic == null) {
          // schema change happened, set statistics which represents all nulls
          columnStatistic = new ColumnStatisticImpl(
              ImmutableMap.of(ColumnStatisticsKind.NULLS_COUNT.getName(), metadata.getStatistic(TableStatistics.ROW_COUNT)),
              getNaturalNullsFirstComparator());
        }
        statisticsList.add(columnStatistic);
      }
      Map<String, Object> statisticsMap = new HashMap<>();
      for (CollectableColumnStatisticKind statisticsKind : statisticsToCollect) {
        Object mergedStatistic = statisticsKind.mergeStatistic(statisticsList);
        statisticsMap.put(statisticsKind.getName(), mergedStatistic);
      }
      columnStatistics.put(column, new ColumnStatisticImpl(statisticsMap, statisticsList.iterator().next().getValueComparator()));
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
                minValue = ParquetMetaStatCollector.convertToDrillDateValue(getInt(minValue));
              }
              if (maxValue != null) {
                maxValue = ParquetMetaStatCollector.convertToDrillDateValue(getInt(maxValue));
              }
              break;
            case DECIMAL:
              if (minValue != null) {
                minValue = BigInteger.valueOf(getInt(minValue));
              }
              if (maxValue != null) {
                maxValue = BigInteger.valueOf(getInt(maxValue));
              }
              break;
            default:
              if (minValue != null) {
                minValue = getInt(minValue);
              }
              if (maxValue != null) {
                maxValue = getInt(maxValue);
              }
          }
        } else {
          if (minValue != null) {
            minValue = getInt(minValue);
          }
          if (maxValue != null) {
            maxValue = getInt(maxValue);
          }
        }
        break;

      case INT64:
        if (originalType == OriginalType.DECIMAL) {
          if (minValue != null) {
            minValue = BigInteger.valueOf(getLong(minValue));
          }
          if (maxValue != null) {
            maxValue = BigInteger.valueOf(getLong(maxValue));
          }
        } else {
          if (minValue != null) {
            minValue = getLong(minValue);
          }
          if (maxValue != null) {
            maxValue = getLong(maxValue);
          }
        }
        break;

      case FLOAT:
        if (minValue != null) {
          minValue = getFloat(minValue);
        }
        if (maxValue != null) {
          maxValue = getFloat(maxValue);
        }
        break;

      case DOUBLE:
        if (minValue != null) {
          minValue = getDouble(minValue);
        }
        if (maxValue != null) {
          maxValue = getDouble(maxValue);
        }
        break;

      case INT96:
        if (minValue != null) {
          minValue = new String(getBytes(minValue));
        }
        if (maxValue  != null) {
          maxValue = new String(getBytes(maxValue));
        }
        break;

      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        if (originalType == OriginalType.DECIMAL) {
          if (minValue != null) {
            minValue = new BigInteger(getBytes(minValue));
          }
          if (maxValue != null) {
            maxValue = new BigInteger(getBytes(maxValue));
          }
        } else if (originalType == OriginalType.INTERVAL) {
          if (minValue != null) {
            minValue = getBytes(minValue);
          }
          if (maxValue != null) {
            maxValue = getBytes(maxValue);
          }
        } else {
          if (minValue != null) {
            minValue = new String(getBytes(minValue));
          }
          if (maxValue  != null) {
            maxValue = new String(getBytes(maxValue));
          }
        }
    }
    return Pair.of(minValue, maxValue);
  }

  private static byte[] getBytes(Object value) {
    if (value instanceof Binary) {
      return ((Binary) value).getBytes();
    } else if (value instanceof byte[]) {
      return (byte[]) value;
    } else if (value instanceof String) { // value is obtained from metadata cache v2+
      return ((String) value).getBytes();
    } else if (value instanceof Map) { // value is obtained from metadata cache v1
      String bytesString = (String) ((Map) value).get("bytes");
      if (bytesString != null) {
        return bytesString.getBytes();
      }
    } else if (value instanceof Long) {
      return Longs.toByteArray((Long) value);
    } else if (value instanceof Integer) {
      return Longs.toByteArray((Integer) value);
    } else if (value instanceof Float) {
      return BigDecimal.valueOf((Float) value).unscaledValue().toByteArray();
    } else if (value instanceof Double) {
      return BigDecimal.valueOf((Double) value).unscaledValue().toByteArray();
    }
    throw new UnsupportedOperationException(String.format("Cannot obtain bytes using value %s", value));
  }

  private static Integer getInt(Object value) {
    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Long) {
      return ((Long) value).intValue();
    } else if (value instanceof Float) {
      return ((Float) value).intValue();
    } else if (value instanceof Double) {
      return ((Double) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt(value.toString());
    } else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).intValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).intValue();
    }
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Long getLong(Object value) {
    if (value instanceof Integer) {
      return Long.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Float) {
      return ((Float) value).longValue();
    } else if (value instanceof Double) {
      return ((Double) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong(value.toString());
    } else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).longValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).longValue();
    }
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Float getFloat(Object value) {
    if (value instanceof Integer) {
      return Float.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return Float.valueOf((Long) value);
    } else if (value instanceof Float) {
      return (Float) value;
    } else if (value instanceof Double) {
      return ((Double) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat(value.toString());
    }
    // TODO: allow conversion form bytes only when actual type of data is known (to obtain scale)
    /* else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).floatValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).floatValue();
    }*/
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Double getDouble(Object value) {
    if (value instanceof Integer) {
      return Double.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return Double.valueOf((Long) value);
    } else if (value instanceof Float) {
      return Double.valueOf((Float) value);
    } else if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof String) {
      return Double.parseDouble(value.toString());
    }
    // TODO: allow conversion form bytes only when actual type of data is known (to obtain scale)
    /* else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).doubleValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).doubleValue();
    }*/
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Comparator getComparator(PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType) {
    if (originalType != null) {
      switch (originalType) {
        case UINT_8:
        case UINT_16:
        case UINT_32:
          return getNaturalNullsFirstComparator();
        case UINT_64:
          return getNaturalNullsFirstComparator();
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
          return getNaturalNullsFirstComparator();
        case INTERVAL:
          return UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
        default:
          return getNaturalNullsFirstComparator();
      }
    } else {
      switch (primitiveType) {
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case BINARY:
        case INT96:
        case FIXED_LEN_BYTE_ARRAY:
          return getNaturalNullsFirstComparator();
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitiveType);
      }
    }
  }

  private static Comparator getComparator(TypeProtos.MinorType type) {
    switch (type) {
      case INTERVALDAY:
      case INTERVAL:
      case INTERVALYEAR:
        return UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
      case UINT1:
        return Comparator.nullsFirst(UnsignedBytes::compare);
      case UINT2:
        // TODO: check whether it will work for Integer::compareUnsigned
      case UINT4:
        return Comparator.nullsFirst(Integer::compareUnsigned);
      case UINT8:
        return Comparator.nullsFirst(Long::compareUnsigned);
      default:
        return getNaturalNullsFirstComparator();
    }
  }

  private static <T extends Comparable<T>> Comparator<T> getNaturalNullsFirstComparator() {
    return Comparator.nullsFirst(Comparator.naturalOrder());
  }

  private static LinkedHashMap<SchemaPath, TypeProtos.MajorType> resolveFields(MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    LinkedHashMap<SchemaPath, TypeProtos.MajorType> columns = new LinkedHashMap<>();
    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      // row groups in the file have the same schema, so using the first one
      LinkedHashMap<SchemaPath, TypeProtos.MajorType> fileColumns = getFileFields(parquetTableMetadata, file);
      for (Map.Entry<SchemaPath, TypeProtos.MajorType> schemaPathMajorType : fileColumns.entrySet()) {

        SchemaPath columnPath = schemaPathMajorType.getKey();
        TypeProtos.MajorType majorType = columns.get(columnPath);
        if (majorType == null) {
          columns.put(columnPath, schemaPathMajorType.getValue());
        } else {
          TypeProtos.MinorType leastRestrictiveType = TypeCastRules.getLeastRestrictiveType(Arrays.asList(majorType.getMinorType(), schemaPathMajorType.getValue().getMinorType()));
          if (leastRestrictiveType != majorType.getMinorType()) {
            columns.put(columnPath, schemaPathMajorType.getValue());
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
    OriginalType originalType = column.getOriginalType();
    // for the case of parquet metadata v1 version, type information isn't stored in parquetTableMetadata, but in ColumnMetadata
    if (originalType == null) {
      originalType = parquetTableMetadata.getOriginalType(column.getName());
    }
    return originalType;
  }

  private static PrimitiveType.PrimitiveTypeName getPrimitiveTypeName(MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ColumnMetadata column) {
    PrimitiveType.PrimitiveTypeName primitiveType = column.getPrimitiveType();
    // for the case of parquet metadata v1 version, type information isn't stored in parquetTableMetadata, but in ColumnMetadata
    if (primitiveType == null) {
      primitiveType = parquetTableMetadata.getPrimitiveType(column.getName());
    }
    return primitiveType;
  }

}
