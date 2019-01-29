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
import org.apache.drill.metastore.expr.StatisticsProvider;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.HashBasedTable;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.MetadataContext;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.metadata.Metadata;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.shaded.guava.com.google.common.collect.Table;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base logic for creating ParquetTableMetadata taken from the ParquetGroupScan.
 */
public class ParquetTableMetadataCreator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetTableMetadataCreator.class);

  private static Object NULL_VALUE = new Object();

  protected List<ReadEntryWithPath> entries;

  protected MetadataBase.ParquetTableMetadataBase parquetTableMetadata;
  private Set<String> fileSet;
  protected final ParquetReaderConfig readerConfig;

  private final ParquetFormatPlugin formatPlugin;
  private final ParquetFormatConfig formatConfig;
  private final DrillFileSystem fs;
  private final MetadataContext metaContext;
  private boolean usedMetadataCache; // false by default
  // may change when filter push down / partition pruning is applied
  private String selectionRoot;
  private String cacheFileRoot;

  private List<SchemaPath> partitionColumns;

  public ParquetTableMetadataCreator(StoragePluginRegistry engineRegistry,
                          String userName,
                          List<ReadEntryWithPath> entries,
                          StoragePluginConfig storageConfig,
                          FormatPluginConfig formatConfig,
                          String selectionRoot,
                          String cacheFileRoot,
                          ParquetReaderConfig readerConfig,
                          Set<String> fileSet) throws IOException, ExecutionSetupException {
    this.entries = entries;
    Preconditions.checkNotNull(storageConfig);
    Preconditions.checkNotNull(formatConfig);
    this.formatPlugin = (ParquetFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    Preconditions.checkNotNull(formatPlugin);
    this.fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.resolveUserName(userName), formatPlugin.getFsConf());
    this.formatConfig = formatPlugin.getConfig();
    this.selectionRoot = selectionRoot;
    this.cacheFileRoot = cacheFileRoot;
    this.metaContext = new MetadataContext();

    this.fileSet = fileSet;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;

    init();
  }

  public ParquetTableMetadataCreator(String userName,
                                     FileSelection selection,
                                     ParquetFormatPlugin formatPlugin,
                                     ParquetReaderConfig readerConfig) throws IOException {
    this.entries = new ArrayList<>();
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;

    this.formatPlugin = formatPlugin;
    this.formatConfig = formatPlugin.getConfig();
    this.fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.resolveUserName(userName), formatPlugin.getFsConf());
    this.selectionRoot = selection.getSelectionRoot();
    this.cacheFileRoot = selection.getCacheFileRoot();

    MetadataContext metadataContext = selection.getMetaContext();
    this.metaContext = metadataContext != null ? metadataContext : new MetadataContext();

    FileSelection fileSelection = expandIfNecessary(selection);
    if (fileSelection != null) {
      if (checkForInitializingEntriesWithSelectionRoot()) {
        // The fully expanded list is already stored as part of the fileSet
        entries.add(new ReadEntryWithPath(fileSelection.getSelectionRoot()));
      } else {
        for (String fileName : fileSelection.getFiles()) {
          entries.add(new ReadEntryWithPath(fileName));
        }
      }

      init();
    }
  }

  public ParquetFormatPlugin getFormatPlugin() {
    return formatPlugin;
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public DrillFileSystem getFs() {
    return fs;
  }

  public boolean isUsedMetadataCache() {
    return usedMetadataCache;
  }

  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  public TableMetadata getTableMetadata() {

    HashMap<String, Object> tableStatistics = new HashMap<>();
    tableStatistics.put(ColumnStatisticsKind.ROW_COUNT.getName(), getRowCount(parquetTableMetadata));

    HashSet<String> partitionKeys = new HashSet<>();

    LinkedHashMap<SchemaPath, TypeProtos.MajorType> fields = resolveFields(parquetTableMetadata);

    HashMap<SchemaPath, ColumnStatistic> columnStatistics = getColumnStatistics(parquetTableMetadata, fields.keySet());

    return new TableMetadata(selectionRoot, selectionRoot,
        fields, columnStatistics, tableStatistics, -1, "root", partitionKeys);
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

  protected PartitionMetadata getPartitionMetadata(SchemaPath logicalExpressions, List<FileMetadata> files) {
    Set<SchemaPath> columns = files.iterator().next().getFields().keySet();
    Map<SchemaPath, MutableLong> nullsCounts = new HashMap<>();
    Map<SchemaPath, Object> minVals = new HashMap<>();
    Map<SchemaPath, Object> maxVals = new HashMap<>();
    Map<SchemaPath, Comparator> valComparator = new HashMap<>();
    Set<String> locations = new HashSet<>();
    for (SchemaPath column : columns) {
      nullsCounts.put(column, new MutableLong());
    }

    long partRowCount = 0;

    for (FileMetadata file : files) {
      locations.add(file.getLocation());
      Long fileRowCount = (Long) file.getStatistic(ColumnStatisticsKind.ROW_COUNT);
      if (fileRowCount != null && fileRowCount != GroupScan.NO_COLUMN_STATS) {
        partRowCount += fileRowCount;
      }
      for (SchemaPath colPath : file.getFields().keySet()) {
        ColumnStatistic columnStatistic = file.getColumnStatistics().get(colPath);

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
      new PartitionMetadata(logicalExpressions, (LinkedHashMap<SchemaPath, TypeProtos.MajorType>) files.iterator().next().getFields(),
          columnStatistics, partStatistics, locations, selectionRoot, -1);
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

    return new FileMetadata(file.getPath(), columns,
        getFileColumnStatistics(file, columns.keySet()), fileStatistics, file.getPath(), selectionRoot, -1);
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

        RowGroupMetadata groupMetadata = new RowGroupMetadata(
            getRowGroupFields(parquetTableMetadata, rowGroupMetadata), columnStatistics,
            rowGroupStatistics, rowGroupMetadata.getHostAffinity(), index++, fileMetadata.getLocation());
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
                minValue = new BigInteger(minValue.toString()).toByteArray();
              }
              if (maxValue != null) {
                maxValue = new BigInteger(maxValue.toString()).toByteArray();
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
            minValue = new BigInteger(minValue.toString()).toByteArray();
          }
          if (maxValue != null) {
            maxValue = new BigInteger(maxValue.toString()).toByteArray();
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

      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        // wrap up into BigInteger to avoid PARQUET-1417
        if (originalType == OriginalType.DECIMAL) {
          if (minValue instanceof Binary) {
            minValue = new BigInteger(((Binary) minValue).getBytes()).toByteArray();
          } else if (minValue instanceof byte[]) {
            minValue = new BigInteger((byte[]) minValue).toByteArray();
          }
          if (maxValue instanceof Binary) {
            maxValue = new BigInteger(((Binary) maxValue).getBytes()).toByteArray();
          } else if (maxValue instanceof byte[]) {
            maxValue = new BigInteger((byte[]) maxValue).toByteArray();
          }
        } else {
          if (minValue instanceof Binary) {
            minValue = ((Binary) minValue).getBytes();
          }
          if (maxValue instanceof Binary) {
            maxValue = ((Binary) maxValue).getBytes();
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
    }
    return Pair.of(minValue, maxValue);
  }

  private static Comparator getComparator(PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType) {
    if (originalType != null) {
      switch (originalType) {
        case UINT_8:
        case UINT_16:
        case UINT_32:
          return StatisticsProvider.UINT32_COMPARATOR;
        case UINT_64:
          return StatisticsProvider.UINT64_COMPARATOR;
        case DATE:
        case INT_8:
        case INT_16:
        case INT_32:
        case INT_64:
        case TIME_MICROS:
        case TIME_MILLIS:
        case TIMESTAMP_MICROS:
        case TIMESTAMP_MILLIS:
          return Comparator.nullsFirst(Comparator.naturalOrder());
        case DECIMAL:
          return StatisticsProvider.BINARY_AS_SIGNED_INTEGER_COMPARATOR;
        case UTF8:
          return StatisticsProvider.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
        case INTERVAL:
          return StatisticsProvider.BINARY_AS_SIGNED_INTEGER_COMPARATOR;
        default:
          return Comparator.nullsFirst((byte[] o1, byte[] o2) ->
              Statistics.getStatsBasedOnType(primitiveType).comparator()
                  .compare(Binary.fromReusedByteArray(o1), Binary.fromReusedByteArray(o2)));
      }
    } else {
      switch (primitiveType) {
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
          return Comparator.nullsFirst(Comparator.naturalOrder());
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return StatisticsProvider.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
        case INT96:
          return StatisticsProvider.BINARY_AS_SIGNED_INTEGER_COMPARATOR;
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

  // parquet group scan methods
  protected void initInternal() throws IOException {
    FileSystem processUserFileSystem = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf());
    Path metaPath = null;
    if (entries.size() == 1 && parquetTableMetadata == null) {
      Path p = Path.getPathWithoutSchemeAndAuthority(new Path(entries.get(0).getPath()));
      if (fs.isDirectory(p)) {
        // Using the metadata file makes sense when querying a directory; otherwise
        // if querying a single file we can look up the metadata directly from the file
        metaPath = new Path(p, Metadata.METADATA_FILENAME);
      }
      if (!metaContext.isMetadataCacheCorrupted() && metaPath != null && fs.exists(metaPath)) {
        parquetTableMetadata = Metadata.readBlockMeta(processUserFileSystem, metaPath, metaContext, readerConfig);
        if (parquetTableMetadata != null) {
          usedMetadataCache = true;
        }
      }
      if (!usedMetadataCache) {
        parquetTableMetadata = Metadata.getParquetTableMetadata(processUserFileSystem, p.toString(), readerConfig);
      }
    } else {
      Path p = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot));
      metaPath = new Path(p, Metadata.METADATA_FILENAME);
      if (!metaContext.isMetadataCacheCorrupted() && fs.isDirectory(new Path(selectionRoot))
        && fs.exists(metaPath)) {
        if (parquetTableMetadata == null) {
          parquetTableMetadata = Metadata.readBlockMeta(processUserFileSystem, metaPath, metaContext, readerConfig);
        }
        if (parquetTableMetadata != null) {
          usedMetadataCache = true;
          if (fileSet != null) {
            parquetTableMetadata = removeUnneededRowGroups(parquetTableMetadata);
          }
        }
      }
      if (!usedMetadataCache) {
        final List<FileStatus> fileStatuses = new ArrayList<>();
        for (ReadEntryWithPath entry : entries) {
          fileStatuses.addAll(
            DrillFileSystemUtil.listFiles(fs, Path.getPathWithoutSchemeAndAuthority(new Path(entry.getPath())), true));
        }

        Map<FileStatus, FileSystem> statusMap = fileStatuses.stream()
          .collect(
            Collectors.toMap(
              Function.identity(),
              s -> processUserFileSystem,
              (oldFs, newFs) -> newFs,
              LinkedHashMap::new));

        parquetTableMetadata = Metadata.getParquetTableMetadata(statusMap, readerConfig);
      }
    }
  }

  protected void init() throws IOException {
    initInternal();

    assert parquetTableMetadata != null;

    if (fileSet == null) {
      fileSet = new HashSet<>();
      fileSet.addAll(parquetTableMetadata.getFiles().stream()
        .map((Function<MetadataBase.ParquetFileMetadata, String>) MetadataBase.ParquetFileMetadata::getPath)
        .collect(Collectors.toSet()));
    }

//    Map<String, CoordinationProtos.DrillbitEndpoint> hostEndpointMap = new HashMap<>();
//
//    for (CoordinationProtos.DrillbitEndpoint endpoint : getDrillbits()) {
//      hostEndpointMap.put(endpoint.getAddress(), endpoint);
//    }
//
//    rowGroupInfos = new ArrayList<>();
//    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
//      int rgIndex = 0;
//      for (RowGroupMetadata rg : file.getRowGroups()) {
//        RowGroupInfo rowGroupInfo =
//          new RowGroupInfo(file.getPath(), rg.getStart(), rg.getLength(), rgIndex, rg.getRowCount());
//        EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
//        rg.getHostAffinity().keySet().stream()
//          .filter(hostEndpointMap::containsKey)
//          .forEach(host ->
//            endpointByteMap.add(hostEndpointMap.get(host), (long) (rg.getHostAffinity().get(host) * rg.getLength())));
//
//        rowGroupInfo.setEndpointByteMap(endpointByteMap);
//        rowGroupInfo.setColumns(rg.getColumns());
//        rgIndex++;
//        rowGroupInfos.add(rowGroupInfo);
//      }
//    }
//
//    this.endpointAffinities = AffinityCreator.getAffinityMap(rowGroupInfos);
//    this.parquetGroupScanStatistics = new ParquetGroupScanStatistics(rowGroupInfos, parquetTableMetadata);
  }
  // overridden protected methods block end

  // private methods block start
  /**
   * Expands the selection's folders if metadata cache is found for the selection root.<br>
   * If the selection has already been expanded or no metadata cache was found, does nothing
   *
   * @param selection actual selection before expansion
   * @return new selection after expansion, if no expansion was done returns the input selection
   */
  private FileSelection expandIfNecessary(FileSelection selection) throws IOException {
    if (selection.isExpandedFully()) {
      return selection;
    }

    // use the cacheFileRoot if provided (e.g after partition pruning)
    Path metaFilePath = new Path(cacheFileRoot != null ? cacheFileRoot : selectionRoot, Metadata.METADATA_FILENAME);
    if (!fs.exists(metaFilePath)) { // no metadata cache
      if (selection.isExpandedPartial()) {
        logger.error("'{}' metadata file does not exist, but metadata directories cache file is present", metaFilePath);
        metaContext.setMetadataCacheCorrupted(true);
      }

      return selection;
    }

    return expandSelectionFromMetadataCache(selection, metaFilePath);
  }

  /**
   * For two cases the entries should be initialized with just the selection root instead of the fully expanded list:
   * <ul>
   *   <li> When metadata caching is corrupted (to use correct file selection)
   *   <li> Metadata caching is correct and used, but pruning was not applicable or was attempted and nothing was pruned
   *        (to reduce overhead in parquet group scan).
   * </ul>
   *
   * @return true if entries should be initialized with selection root, false otherwise
   */
  private boolean checkForInitializingEntriesWithSelectionRoot() {
    // TODO: at some point we should examine whether the list of entries is absolutely needed.
    return metaContext.isMetadataCacheCorrupted() || (parquetTableMetadata != null &&
      (metaContext.getPruneStatus() == MetadataContext.PruneStatus.NOT_STARTED || metaContext.getPruneStatus() == MetadataContext.PruneStatus.NOT_PRUNED));
  }

  /**
   * Create and return a new file selection based on reading the metadata cache file.
   *
   * This function also initializes a few of ParquetGroupScan's fields as appropriate.
   *
   * @param selection initial file selection
   * @param metaFilePath metadata cache file path
   * @return file selection read from cache
   *
   * @throws org.apache.drill.common.exceptions.UserException when the updated selection is empty, this happens if the user selects an empty folder.
   */
  private FileSelection expandSelectionFromMetadataCache(FileSelection selection, Path metaFilePath) throws IOException {
    // get the metadata for the root directory by reading the metadata file
    // parquetTableMetadata contains the metadata for all files in the selection root folder, but we need to make sure
    // we only select the files that are part of selection (by setting fileSet appropriately)

    // get (and set internal field) the metadata for the directory by reading the metadata file
    FileSystem processUserFileSystem = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf());
    parquetTableMetadata = Metadata.readBlockMeta(processUserFileSystem, metaFilePath, metaContext, readerConfig);
    if (ignoreExpandingSelection(parquetTableMetadata)) {
      return selection;
    }
    if (formatConfig.areCorruptDatesAutoCorrected()) {
      ParquetReaderUtility.correctDatesInMetadataCache(this.parquetTableMetadata);
    }
    ParquetReaderUtility.transformBinaryInMetadataCache(parquetTableMetadata, readerConfig);
    List<FileStatus> fileStatuses = selection.getStatuses(fs);

    if (fileSet == null) {
      fileSet = Sets.newHashSet();
    }

    final Path first = fileStatuses.get(0).getPath();
    if (fileStatuses.size() == 1 && selection.getSelectionRoot().equals(first.toString())) {
      // we are selecting all files from selection root. Expand the file list from the cache
      for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        fileSet.add(file.getPath());
      }

    } else if (selection.isExpandedPartial() && !selection.hadWildcard() && cacheFileRoot != null) {
      if (selection.wasAllPartitionsPruned()) {
        // if all partitions were previously pruned, we only need to read 1 file (for the schema)
        fileSet.add(this.parquetTableMetadata.getFiles().get(0).getPath());
      } else {
        // we are here if the selection is in the expanded_partial state (i.e it has directories).  We get the
        // list of files from the metadata cache file that is present in the cacheFileRoot directory and populate
        // the fileSet. However, this is *not* the final list of files that will be scanned in execution since the
        // second phase of partition pruning will apply on the files and modify the file selection appropriately.
        for (MetadataBase.ParquetFileMetadata file : this.parquetTableMetadata.getFiles()) {
          fileSet.add(file.getPath());
        }
      }
    } else {
      // we need to expand the files from fileStatuses
      for (FileStatus status : fileStatuses) {
        Path cacheFileRoot = status.getPath();
        if (status.isDirectory()) {
          //TODO [DRILL-4496] read the metadata cache files in parallel
          final Path metaPath = new Path(cacheFileRoot, Metadata.METADATA_FILENAME);
          final MetadataBase.ParquetTableMetadataBase metadata = Metadata.readBlockMeta(processUserFileSystem, metaPath, metaContext, readerConfig);
          if (ignoreExpandingSelection(metadata)) {
            return selection;
          }
          for (MetadataBase.ParquetFileMetadata file : metadata.getFiles()) {
            fileSet.add(file.getPath());
          }
        } else {
          final Path path = Path.getPathWithoutSchemeAndAuthority(cacheFileRoot);
          fileSet.add(path.toString());
        }
      }
    }

    if (fileSet.isEmpty()) {
      // no files were found, most likely we tried to query some empty sub folders
      logger.warn("The table is empty but with outdated invalid metadata cache files. Please, delete them.");
      return null;
    }

    List<String> fileNames = new ArrayList<>(fileSet);

    // when creating the file selection, set the selection root without the URI prefix
    // The reason is that the file names above have been created in the form
    // /a/b/c.parquet and the format of the selection root must match that of the file names
    // otherwise downstream operations such as partition pruning can break.
    final Path metaRootPath = Path.getPathWithoutSchemeAndAuthority(new Path(selection.getSelectionRoot()));
    this.selectionRoot = metaRootPath.toString();

    // Use the FileSelection constructor directly here instead of the FileSelection.create() method
    // because create() changes the root to include the scheme and authority; In future, if create()
    // is the preferred way to instantiate a file selection, we may need to do something different...
    // WARNING: file statuses and file names are inconsistent
    FileSelection newSelection = new FileSelection(selection.getStatuses(fs), fileNames, metaRootPath.toString(),
      cacheFileRoot, selection.wasAllPartitionsPruned());

    newSelection.setExpandedFully();
    newSelection.setMetaContext(metaContext);
    return newSelection;
  }

  private MetadataBase.ParquetTableMetadataBase removeUnneededRowGroups(MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    List<MetadataBase.ParquetFileMetadata> newFileMetadataList = new ArrayList<>();
    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      if (fileSet.contains(file.getPath())) {
        newFileMetadataList.add(file);
      }
    }

    MetadataBase.ParquetTableMetadataBase metadata = parquetTableMetadata.clone();
    metadata.assignFiles(newFileMetadataList);
    return metadata;
  }

  /**
   * If metadata is corrupted, ignore expanding selection and reset parquetTableMetadata and fileSet fields
   *
   * @param metadata parquet table metadata
   * @return true if parquet metadata is corrupted, false otherwise
   */
  private boolean ignoreExpandingSelection(MetadataBase.ParquetTableMetadataBase metadata) {
    if (metadata == null || metaContext.isMetadataCacheCorrupted()) {
      logger.debug("Selection can't be expanded since metadata file is corrupted or metadata version is not supported");
      this.parquetTableMetadata = null;
      this.fileSet = null;
      return true;
    }
    return false;
  }
  // private methods block end

}
