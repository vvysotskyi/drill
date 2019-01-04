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
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.TableStatistics;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.BaseMetadataGroupScan;
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
import org.apache.drill.metastore.BaseMetadata;
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

public abstract class AbstractParquetGroupScan extends BaseMetadataGroupScan {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetGroupScan.class);

  protected List<ReadEntryWithPath> entries;
  protected List<RowGroupMetadata> rowGroups = new ArrayList<>();
  protected Map<Integer, Long> rowGroupAndNumsToRead = new HashMap<>();

//  protected ParquetTableMetadataBase parquetTableMetadata;
//  private List<RowGroupInfo> rowGroupInfos;
  protected ListMultimap<Integer, RowGroupInfo> mappings;
  protected Set<String> fileSet;
  protected ParquetReaderConfig readerConfig;

  private List<EndpointAffinity> endpointAffinities;
//  private ParquetGroupScanStatistics parquetGroupScanStatistics;

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
    this.rowGroups = that.rowGroups;
    this.files = that.files;
    this.tableMetadata = that.tableMetadata;
    this.partitions = that.partitions;
//    this.parquetTableMetadata = that.parquetTableMetadata;
//    this.rowGroupInfos = that.rowGroupInfos == null ? null : new ArrayList<>(that.rowGroupInfos);
    this.endpointAffinities = that.endpointAffinities == null ? null : new ArrayList<>(that.endpointAffinities);
    this.mappings = that.mappings == null ? null : ArrayListMultimap.create(that.mappings);
//    this.parquetGroupScanStatistics = that.parquetGroupScanStatistics == null ? null : new ParquetGroupScanStatistics(that.parquetGroupScanStatistics);
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
    if (files == null) {
      return entries;
    }
    return files.stream()
        .map(fileMetadata -> new ReadEntryWithPath(fileMetadata.getLocation()))
        .collect(Collectors.toList());
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
    return rowGroups.size();
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
  public BaseMetadataGroupScan applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
      FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {
    // Builds filter for pruning. If filter cannot be built, null should be returned.
    FilterPredicate filterPredicate = getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, true, tableMetadata.getFields());
    if (filterPredicate == null) {
      logger.debug("FilterPredicate cannot be built.");
      return null;
    }

    Set<SchemaPath> schemaPathsInExpr =
        filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);

    RowGroupScanBuilder builder = getBuilder();

    filterTableMetadata(filterPredicate, schemaPathsInExpr, builder);

    filterPartitionMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);

    filterFileMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);

    filterRowGroupMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);

    if (builder.rowGroups != null && builder.rowGroups.size() == rowGroups.size()) {
      // There is no reduction of files. Return the original groupScan.
      logger.debug("applyFilter() does not have any pruning!");
      matchAllRowGroups = builder.isMatchAllRowGroups();
      return null;
    } else if (!builder.isMatchAllRowGroups()
        && builder.getOverflowLevel() == MetadataLevel.NONE
        && (builder.getTableMetadata().isEmpty() || ((builder.getPartitions() == null
            || builder.getPartitions().isEmpty()) && partitions.size() > 0) || builder.getFiles() == null
            || builder.getFiles().isEmpty() || builder.rowGroups == null)) {
      if (rowGroups.size() == 1) {
        // For the case when group scan has single row group and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      logger.debug("All row groups have been filtered out. Add back one to get schema from scanner");
      FileMetadata nextFile = files.iterator().next();
      RowGroupMetadata nextRowGroup = rowGroups.iterator().next();
      builder.withRowGroups(Collections.singletonList(nextRowGroup))
          .withTable(Collections.singletonList(tableMetadata))
          .withMatching(false)
          .withPartitions(partitions.size() > 0 ? Collections.singletonList(partitions.iterator().next()) : Collections.emptyList())
          .withFiles(Collections.singletonList(nextFile));
    }

    logger.debug("applyFilter {} reduce row groups # from {} to {}",
        ExpressionStringBuilder.toString(filterExpr), rowGroups.size(), builder.rowGroups.size());

    return builder.build();
  }

  // narrows the return type
  protected abstract RowGroupScanBuilder getBuilder();

  protected void filterRowGroupMetadata(OptionManager optionManager, FilterPredicate filterPredicate,
                                        Set<SchemaPath> schemaPathsInExpr, RowGroupScanBuilder builder) {
    if (builder.getFiles() != null && builder.getFiles().size() > 0) {
      List<RowGroupMetadata> prunedRowGroups;
      if (files.size() == builder.getFiles().size()) {
        // no partition pruning happened, no need to prune initial files list
        prunedRowGroups = rowGroups;
      } else {
        // prunes files to leave only files which are contained by pruned partitions
        prunedRowGroups = pruneRowGroupsForFiles(builder.getFiles());
      }

      if (builder.isMatchAllRowGroups()) {
        builder.withRowGroups(prunedRowGroups);
        return;
      }

      // Stop files pruning for the case:
      //    -  # of files is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      if (prunedRowGroups.size() <= optionManager.getOption(
          PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {

        boolean matchAllRowGroupsLocal = matchAllRowGroups;
        matchAllRowGroups = true;

        List<RowGroupMetadata> filteredRowGroups = filterAndGetMetadata(schemaPathsInExpr, prunedRowGroups, filterPredicate);

        builder.withRowGroups(filteredRowGroups)
            .withMatching(matchAllRowGroups);

        matchAllRowGroups = matchAllRowGroupsLocal;
      } else {
        builder.withRowGroups(prunedRowGroups)
          .withMatching(false)
          .withOverflow(MetadataLevel.FILE);
      }
    }
  }

  protected List<RowGroupMetadata> pruneRowGroupsForFiles(List<FileMetadata> filteredPartitionMetadata) {
    List<RowGroupMetadata> prunedFiles = new ArrayList<>();
//    rowGroups.entries().stream()
//        .filter(entry -> filteredPartitionMetadata.contains(entry.getKey()))
//        .forEach(entry -> prunedFiles.put(entry.getKey(), entry.getValue()));

    for (FileMetadata filteredPartition : filteredPartitionMetadata) {
      for (RowGroupMetadata file : rowGroups) {
        if (file.getLocation().startsWith(filteredPartition.getLocation())) {
          prunedFiles.add(file);
        }
      }
    }

    return prunedFiles;
  }
  // filter push down methods block end

  // limit push down methods start
  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    // further optimization : minimize # of files chosen, or the affinity of files chosen.

    long tableRowCount = (long) tableMetadata.getStatistic(TableStatistics.ROW_COUNT);
    if (tableRowCount <= maxRecords) {
      logger.debug("limit push down does not apply, since total number of rows [{}] is less or equal to the required [{}].",
        tableRowCount, maxRecords);
      return null;
    }

    // Calculate number of files to read based on maxRecords and update
    // number of records to read for each of those rowGroups.
    List<RowGroupMetadata> qualifiedRowGroups = new ArrayList<>();
    int currentRowCount = 0;
    for (RowGroupMetadata rowGroupInfo : rowGroups) {
      long rowCount = (long) rowGroupInfo.getStatistic(ColumnStatisticsKind.ROW_COUNT);
      if (currentRowCount + rowCount <= maxRecords) {
        currentRowCount += rowCount;
//        rowGroupInfo.setNumRecordsToRead(rowCount);
        qualifiedRowGroups.add(rowGroupInfo);
        continue;
      } else if (currentRowCount < maxRecords) {
//        rowGroupInfo.setNumRecordsToRead(maxRecords - currentRowCount);
        qualifiedRowGroups.add(rowGroupInfo);
      }
      break;
    }

    if (rowGroups.size() == qualifiedRowGroups.size()) {
      logger.debug("limit push down does not apply, since number of row groups was not reduced.");
      return null;
    }

    logger.debug("applyLimit() reduce files # from {} to {}.", rowGroups.size(), qualifiedRowGroups.size());

//    return getBuilder()
//      .withRowGroups(qualifiedRowGroups)
//      .withFiles()
//      .build();
    return super.applyLimit(maxRecords);
  }
  // limit push down methods end

  // partition pruning methods start
//  @Override
//  public List<SchemaPath> getPartitionColumns() {
//    return parquetGroupScanStatistics.getPartitionColumns();
//  }
//
//  @JsonIgnore
//  public <T> T getPartitionValue(String path, SchemaPath column, Class<T> clazz) {
//    return clazz.cast(parquetGroupScanStatistics.getPartitionValue(path, column));
//  }
  // partition pruning methods end

  // helper method used for partition pruning and filter push down
  @Override
  public void modifyFileSelection(FileSelection selection) {
    // TODO: make this method as deprecated and remove its usage
    super.modifyFileSelection(selection);

    List<String> files = selection.getFiles();
    fileSet = new HashSet<>(files);
    entries = new ArrayList<>(files.size());

    entries.addAll(files.stream()
        .map(ReadEntryWithPath::new)
        .collect(Collectors.toList()));

    rowGroups = rowGroups.stream()
        .filter(entry -> fileSet.contains(entry.getLocation()))
        .collect(Collectors.toList());
  }


  // protected methods block
  protected void init() throws IOException {
    super.init();

    this.endpointAffinities = AffinityCreator.getAffinityMap(getRowGroupInfos());
  }

  // abstract methods block start
  protected abstract void initInternal() throws IOException;
  protected abstract Collection<CoordinationProtos.DrillbitEndpoint> getDrillbits();
  protected abstract AbstractParquetGroupScan cloneWithFileSelection(Collection<String> filePaths) throws IOException;
  protected abstract AbstractParquetGroupScan cloneWithRowGroups(Multimap<FileMetadata, ? extends BaseMetadata> rowGroups) throws IOException;
  protected abstract List<String> getPartitionValues(RowGroupInfo rowGroupInfo);
  // abstract methods block end

  protected static abstract class RowGroupScanBuilder extends GroupScanBuilder {
    protected List<RowGroupMetadata> rowGroups;

    public RowGroupScanBuilder withRowGroups(List<RowGroupMetadata> rowGroups) {
      this.rowGroups = rowGroups;
      return this;
    }
  }

}
