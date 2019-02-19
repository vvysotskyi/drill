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

import org.apache.drill.exec.physical.base.ParquetTableMetadataProvider;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.TableStatistics;
import org.apache.drill.shaded.guava.com.google.common.collect.HashBasedTable;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimaps;
import org.apache.drill.shaded.guava.com.google.common.collect.SetMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Table;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.metastore.ColumnStatistic;
import org.apache.drill.metastore.ColumnStatisticImpl;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.metastore.TableMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseParquetTableMetadataProvider implements ParquetTableMetadataProvider {

  static final Object NULL_VALUE = new Object();

  private ParquetGroupScanStatistics<? extends BaseMetadata> parquetGroupScanStatistics;

  protected final List<ReadEntryWithPath> entries;
  protected final ParquetReaderConfig readerConfig;

  protected MetadataBase.ParquetTableMetadataBase parquetTableMetadata;
  protected Set<String> fileSet;

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
      Map<String, Object> tableStatistics = new HashMap<>();
      Set<String> partitionKeys = new HashSet<>();
      Map<SchemaPath, TypeProtos.MajorType> fields = ParquetTableMetadataUtils.resolveFields(parquetTableMetadata);

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
        columnStatistics = ParquetTableMetadataUtils.getColumnStatistics(metadata, fields.keySet(), ParquetTableMetadataUtils.PARQUET_STATISTICS);
      } else {
        columnStatistics = new HashMap<>();
        tableStatistics.put(TableStatistics.ROW_COUNT.getName(), getParquetGroupScanStatistics().getRowCount());

        for (SchemaPath partitionColumn : fields.keySet()) {
          long columnValueCount = getParquetGroupScanStatistics().getColumnValueCount(partitionColumn);
          ImmutableMap<String, Long> stats = ImmutableMap.of(
              TableStatistics.ROW_COUNT.getName(), columnValueCount,
              ColumnStatisticsKind.NULLS_COUNT.getName(), getParquetGroupScanStatistics().getRowCount() - columnValueCount);
          columnStatistics.put(partitionColumn, new ColumnStatisticImpl(stats, ParquetTableMetadataUtils.getNaturalNullsFirstComparator()));
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
            partitions.add(ParquetTableMetadataUtils.getPartitionMetadata(logicalExpressions, partValues, tableName));
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
                    ParquetTableMetadataUtils.getComparator(getParquetGroupScanStatistics().getTypeForColumn(partitionColumn).getMinorType())));

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
          RowGroupMetadata rowGroupMetadata = ParquetTableMetadataUtils.getRowGroupMetadata(parquetTableMetadata, rowGroup, index++, file.getPath());
          fileRowGroups.add(rowGroupMetadata);

          if (addRowGroups) {
            rowGroups.add(rowGroupMetadata);
          }
        }

        FileMetadata fileMetadata = ParquetTableMetadataUtils.getFileMetadata(fileRowGroups, tableName);
        files.add(fileMetadata);
      }
    }
    return files;
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
      rowGroups = ParquetTableMetadataUtils.getRowGroupsMetadata(parquetTableMetadata);
    }
    return rowGroups;
  }

  protected abstract void initInternal() throws IOException;
}