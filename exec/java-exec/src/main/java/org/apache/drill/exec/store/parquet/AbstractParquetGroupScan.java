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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.physical.base.AbstractGroupScanWithMetadata;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.TableStatistics;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.metastore.expr.FilterPredicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class AbstractParquetGroupScan extends AbstractGroupScanWithMetadata {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetGroupScan.class);

  protected List<ReadEntryWithPath> entries;
  protected List<RowGroupMetadata> rowGroups = new ArrayList<>();
  protected Map<Integer, Long> rowGroupAndNumsToRead = new HashMap<>();

  protected ListMultimap<Integer, RowGroupInfo> mappings;
  protected ParquetReaderConfig readerConfig;

  private List<EndpointAffinity> endpointAffinities;

  protected AbstractParquetGroupScan(String userName,
                                     List<SchemaPath> columns,
                                     List<ReadEntryWithPath> entries,
                                     ParquetReaderConfig readerConfig,
                                     LogicalExpression filter) {
    super(userName, columns, filter);
    this.entries = entries;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
  }

  // immutable copy constructor
  protected AbstractParquetGroupScan(AbstractParquetGroupScan that) {
    super(that.getUserName(), that.getColumns(), that.getFilter());
    this.partitionColumns = that.partitionColumns;
    this.rowGroups = that.rowGroups;
    this.files = that.files;
    this.tableMetadata = that.tableMetadata;
    this.partitions = that.partitions;
    this.endpointAffinities = that.endpointAffinities == null ? null : new ArrayList<>(that.endpointAffinities);
    this.mappings = that.mappings == null ? null : ArrayListMultimap.create(that.mappings);
    this.fileSet = that.fileSet == null ? null : new HashSet<>(that.fileSet);
    this.entries = that.entries == null ? null : new ArrayList<>(that.entries);
    this.readerConfig = that.readerConfig;
  }

  @Override
  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public List<ReadEntryWithPath> getEntries() {
    // master version has incorrect behavior: all files list is known and stored in metadata,
    // but it is ignored and is affected only when pruning is happened.
    // Is it done in order to decrease plan?
//    if (files == null || files.isEmpty()) {
    return entries;
//    }
//    return files.stream()
//        .map(fileMetadata -> new ReadEntryWithPath(fileMetadata.getLocation()))
//        .collect(Collectors.toList());
  }

  @JsonProperty("readerConfig")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // do not serialize reader config if it contains all default values
  public ParquetReaderConfig getReaderConfigForSerialization() {
    return ParquetReaderConfig.getDefaultInstance().equals(readerConfig) ? null : readerConfig;
  }

  @JsonIgnore
  public ParquetReaderConfig getReaderConfig() {
    return readerConfig;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  /**
   * Calculates the affinity each endpoint has for this scan,
   * by adding up the affinity each endpoint has for each rowGroup.
   *
   * @return a list of EndpointAffinity objects
   */
  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return endpointAffinities;
  }

  @Override
  public ScanStats getScanStats() {
    int columnCount = columns == null ? 20 : columns.size();
    // TODO: add check for metadata availability and use tableMetadata with updated rows count
    long rowCount = 0;
    for (RowGroupMetadata rowGroup : rowGroups) {
      rowCount += (long) rowGroup.getStatistic(TableStatistics.ROW_COUNT);
    }

    return new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, rowCount, 1, rowCount * columnCount);
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> incomingEndpoints) {
    this.mappings = AssignmentCreator.getMappings(incomingEndpoints, getRowGroupInfos());
  }

  // TODO: rework to store once and update where needed
  private List<RowGroupInfo> getRowGroupInfos() {
    Map<String, CoordinationProtos.DrillbitEndpoint> hostEndpointMap = new HashMap<>();

    for (CoordinationProtos.DrillbitEndpoint endpoint : getDrillbits()) {
      hostEndpointMap.put(endpoint.getAddress(), endpoint);
    }
    AtomicInteger rgIndex = new AtomicInteger();

    // TODO: investigate how to handle such case.
    //  Perhaps approach used for files should be used in such case.
    if (rowGroups == null) {
      return null;
    }

    return rowGroups.stream()
        .map(rowGroupMetadata -> {
          RowGroupInfo rowGroupInfo = new RowGroupInfo(rowGroupMetadata.getLocation(),
              (long) rowGroupMetadata.getStatistic(() -> "start"),
              (long) rowGroupMetadata.getStatistic(() -> "length"),
              rowGroupMetadata.getRowGroupIndex(),
              (long) rowGroupMetadata.getStatistic(ColumnStatisticsKind.ROW_COUNT));
          rowGroupInfo.setNumRecordsToRead(
              rowGroupAndNumsToRead.getOrDefault(rgIndex.getAndIncrement(), rowGroupInfo.getRowCount()));

          EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
          rowGroupMetadata.getHostAffinity().keySet().stream()
              .filter(hostEndpointMap::containsKey)
              .forEach(host ->
                  endpointByteMap.add(hostEndpointMap.get(host),
                      (long) (rowGroupMetadata.getHostAffinity().get(host) * (long) rowGroupMetadata.getStatistic(() -> "length"))));

          rowGroupInfo.setEndpointByteMap(endpointByteMap);

          return rowGroupInfo;
        })
        .collect(Collectors.toList());
  }

  @Override
  public int getMaxParallelizationWidth() {
    if (rowGroups != null) {
      return rowGroups.size();
    } else {
      if (files != null) {
        return files.size();
      } else {
        return partitions != null ? partitions.size() : 1;
      }
    }
  }

  protected List<RowGroupReadEntry> getReadEntries(int minorFragmentId) {
    assert minorFragmentId < mappings.size() : String
        .format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.",
            mappings.size(), minorFragmentId);

    List<RowGroupInfo> rowGroupsForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!rowGroupsForMinor.isEmpty(),
        String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    List<RowGroupReadEntry> entries = new ArrayList<>();
    for (RowGroupInfo rgi : rowGroupsForMinor) {
      RowGroupReadEntry entry = new RowGroupReadEntry(rgi.getPath(), rgi.getStart(),
          rgi.getLength(), rgi.getRowGroupIndex(),
          rowGroupAndNumsToRead.getOrDefault(rgi.getRowGroupIndex(), rgi.getNumRecordsToRead())
      );
      entries.add(entry);
    }
    return entries;
  }

  @Override
  public AbstractGroupScanWithMetadata applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
      FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {
    // Builds filter for pruning. If filter cannot be built, null should be returned.
    FilterPredicate filterPredicate = getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, optionManager, true);
    if (filterPredicate == null) {
      logger.debug("FilterPredicate cannot be built.");
      return null;
    }

    Set<SchemaPath> schemaPathsInExpr =
        filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);

    RowGroupScanBuilder builder = getFiltered(optionManager, filterPredicate, schemaPathsInExpr);

    if (rowGroups != null) {
      if (builder.rowGroups != null && rowGroups.size() == builder.rowGroups.size()) {
        // There is no reduction of files
        logger.debug("applyFilter() does not have any pruning");
        matchAllRowGroups = builder.isMatchAllRowGroups();
        return null;
      }
    } else if (files != null) {
      if (builder.getFiles() != null && files.size() == builder.getFiles().size()) {
        // There is no reduction of files
        logger.debug("applyFilter() does not have any pruning");
        matchAllRowGroups = builder.isMatchAllRowGroups();
        return null;
      }
    } else if (partitions != null) {
      if (builder.getPartitions() != null && partitions.size() == builder.getPartitions().size()) {
        // There is no reduction of partitions
        logger.debug("applyFilter() does not have any pruning ");
        matchAllRowGroups = builder.isMatchAllRowGroups();
        return null;
      }
    } else if (tableMetadata != null) {
      // There is no reduction
      logger.debug("applyFilter() does not have any pruning");
      matchAllRowGroups = builder.isMatchAllRowGroups();
      return null;
    }

    if (!builder.isMatchAllRowGroups()
        // filter returns empty result using table metadata
        && (((builder.getTableMetadata() == null || builder.getTableMetadata().isEmpty()) && tableMetadata != null)
            // all partitions pruned if partition metadata is available
            || unchangedMetadata(partitions, builder.getPartitions())
            // all files are pruned if file metadata is available
            || unchangedMetadata(files, builder.getFiles())
            // all row groups are pruned if row group metadata is available
            || unchangedMetadata(rowGroups, builder.rowGroups))) {
      if (rowGroups.size() == 1) {
        // For the case when group scan has single row group and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      logger.debug("All row groups have been filtered out. Add back one to get schema from scanner");
      builder.withRowGroups(getNextOrEmpty(rowGroups))
          .withTable(tableMetadata != null ? Collections.singletonList(tableMetadata) : Collections.emptyList())
          .withPartitions(getNextOrEmpty(partitions))
          .withFiles(getNextOrEmpty(files))
          .withMatching(false);
    }

//    logger.debug("applyFilter {} reduce row groups # from {} to {}",
//        ExpressionStringBuilder.toString(filterExpr), rowGroups.size(), builder.rowGroups.size());

    return builder.build();
  }

  @Override
  protected RowGroupScanBuilder getFiltered(OptionManager optionManager, FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr) {
    RowGroupScanBuilder builder = (RowGroupScanBuilder) super.getFiltered(optionManager, filterPredicate, schemaPathsInExpr);

    if (rowGroups != null) {
      filterRowGroupMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);
    }
    return builder;
  }

  @Override
  protected TupleMetadata getColumnMetadata() {
    TupleMetadata columnMetadata = super.getColumnMetadata();
    if (columnMetadata == null && rowGroups != null && !rowGroups.isEmpty()) {
      return rowGroups.iterator().next().getSchema();
    }
    return columnMetadata;
  }

  // narrows the return type
  protected abstract RowGroupScanBuilder getBuilder();

  protected void filterRowGroupMetadata(OptionManager optionManager, FilterPredicate filterPredicate,
                                        Set<SchemaPath> schemaPathsInExpr, RowGroupScanBuilder builder) {
    List<RowGroupMetadata> prunedRowGroups;
    if (files != null && !files.isEmpty() && files.size() > builder.getFiles().size()) {
      // prunes files to leave only files which are contained by pruned partitions
      prunedRowGroups = pruneRowGroupsForFiles(builder.getFiles());
    } else if (partitions != null && !partitions.isEmpty() && partitions.size() > builder.getPartitions().size()) {
      prunedRowGroups = pruneForPartitions(rowGroups, builder.getPartitions());
    } else {
      // no partition pruning happened, no need to prune initial files list
      prunedRowGroups = rowGroups;
    }

    if (builder.isMatchAllRowGroups()) {
      builder.withRowGroups(prunedRowGroups);
      return;
    }

    // Stop files pruning for the case:
    //    -  # of row groups is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
    if (prunedRowGroups.size() <= optionManager.getOption(
      PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {

      boolean matchAllRowGroupsLocal = matchAllRowGroups;
      matchAllRowGroups = true;

      List<RowGroupMetadata> filteredRowGroups = filterAndGetMetadata(schemaPathsInExpr, prunedRowGroups, filterPredicate, optionManager);

      builder.withRowGroups(filteredRowGroups)
          .withMatching(matchAllRowGroups);

      matchAllRowGroups = matchAllRowGroupsLocal;
    } else {
      builder.withRowGroups(prunedRowGroups)
          .withMatching(false)
          .withOverflow(MetadataLevel.FILE);
    }
  }

  private List<RowGroupMetadata> pruneRowGroupsForFiles(List<FileMetadata> filteredFileMetadata) {
    List<RowGroupMetadata> prunedRowGroups = new ArrayList<>();
    for (RowGroupMetadata file : rowGroups) {
      for (FileMetadata filteredPartition : filteredFileMetadata) {
        if (file.getLocation().startsWith(filteredPartition.getLocation())) {
          prunedRowGroups.add(file);
          break;
        }
      }
    }

    return prunedRowGroups;
  }

  // filter push down methods block end

  // limit push down methods start
  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    RowGroupScanBuilder builder = (RowGroupScanBuilder) limitFiles(maxRecords);

    if (builder.getTableMetadata() == null && tableMetadata != null) {
      logger.debug("limit push down does not apply, since table has less rows.");
      return null;
    }

    List<FileMetadata> qualifiedFiles = builder.getFiles();
    qualifiedFiles = qualifiedFiles != null ? qualifiedFiles : Collections.emptyList();

    List<RowGroupMetadata> qualifiedRowGroups = !qualifiedFiles.isEmpty() ? pruneRowGroupsForFiles(qualifiedFiles.subList(0, qualifiedFiles.size() - 1)) : new ArrayList<>();

    // get row groups of the last file to filter them or get all row groups if files metadata isn't available
    List<RowGroupMetadata> lastFileRowGroups = !qualifiedFiles.isEmpty() ? pruneRowGroupsForFiles(qualifiedFiles.subList(qualifiedFiles.size() - 1, qualifiedFiles.size())) : rowGroups;

    qualifiedRowGroups.addAll(limitMetadata(lastFileRowGroups, maxRecords));

    if (rowGroups != null && rowGroups.size() == qualifiedRowGroups.size()) {
      logger.debug("limit push down does not apply, since number of row groups was not reduced.");
      return null;
    }

    return builder
        .withRowGroups(qualifiedRowGroups)
        .build();
  }
  // limit push down methods end

  // helper method used for partition pruning and filter push down
  @Override
  public void modifyFileSelection(FileSelection selection) {
    super.modifyFileSelection(selection);

    List<String> files = selection.getFiles();
    fileSet = new HashSet<>(files);
    entries = new ArrayList<>(files.size());

    entries.addAll(files.stream()
        .map(ReadEntryWithPath::new)
        .collect(Collectors.toList()));

    if (rowGroups != null) {
      rowGroups = rowGroups.stream()
          .filter(entry -> fileSet.contains(entry.getLocation()))
          .collect(Collectors.toList());
    }

    if (this.files != null) {
      this.files = this.files.stream()
          .filter(entry -> fileSet.contains(entry.getLocation()))
          .collect(Collectors.toList());
    }

    if (partitions != null) {
      Set<PartitionMetadata> newPartitions = new HashSet<>();
      for (PartitionMetadata entry : partitions) {
        for (String partLocation : entry.getLocations()) {
          if (fileSet.contains(partLocation)) {
            newPartitions.add(entry);
          }
        }
      }
      partitions = new ArrayList<>(newPartitions);
    }
  }

  // protected methods block
  @Override
  protected void init() throws IOException {
    super.init();

    this.endpointAffinities = AffinityCreator.getAffinityMap(getRowGroupInfos());
  }

  // abstract methods block start
  protected abstract Collection<CoordinationProtos.DrillbitEndpoint> getDrillbits();
  protected abstract AbstractParquetGroupScan cloneWithFileSelection(Collection<String> filePaths) throws IOException;
  // abstract methods block end

  protected abstract static class RowGroupScanBuilder extends GroupScanWithMetadataBuilder {
    protected AbstractParquetGroupScan newScan;
    protected List<RowGroupMetadata> rowGroups;

    public RowGroupScanBuilder withRowGroups(List<RowGroupMetadata> rowGroups) {
      this.rowGroups = rowGroups;
      return this;
    }

    @Override
    public AbstractGroupScanWithMetadata build() {
      newScan.tableMetadata = tableMetadata != null && !tableMetadata.isEmpty() ? tableMetadata.get(0) : null;
      newScan.partitions = partitions != null ? partitions : Collections.emptyList();
      newScan.files = files != null ? files : Collections.emptyList();
      newScan.rowGroups = rowGroups != null ? rowGroups : Collections.emptyList();
      newScan.matchAllRowGroups = matchAllRowGroups;
      // since builder is used when pruning happens, entries and fileSet should be expanded
      if (!newScan.files.isEmpty()) {
        newScan.entries = newScan.files.stream()
            .map(file -> new ReadEntryWithPath(file.getLocation()))
            .collect(Collectors.toList());
      } else if (!newScan.rowGroups.isEmpty()) {
        newScan.entries = newScan.rowGroups.stream()
            .map(RowGroupMetadata::getLocation)
            .distinct()
            .map(ReadEntryWithPath::new)
            .collect(Collectors.toList());
      }

      newScan.fileSet = newScan.files.stream()
          .map(FileMetadata::getLocation)
          .collect(Collectors.toSet());

      return newScan;
    }
  }

}
