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
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimaps;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class AbstractParquetGroupScan extends BaseMetadataGroupScan {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetGroupScan.class);

//  protected List<ReadEntryWithPath> entries;
  protected Multimap<FileMetadata, RowGroupMetadata> rowGroups = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
  protected Map<Integer, Long> rowGroupAndNumsToRead =  new HashMap<>();

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
//    this.entries = entries;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
  }

  // immutable copy constructor
  protected AbstractParquetGroupScan(AbstractParquetGroupScan that) {
    super(that.getUserName(), that.getColumns(), that.getFilter());
    this.rowGroups = that.rowGroups;
    this.files = that.files;
    this.tableMetadata = that.tableMetadata;
    this.fileSet = that.fileSet;
    this.partitions = that.partitions;
//    this.parquetTableMetadata = that.parquetTableMetadata;
//    this.rowGroupInfos = that.rowGroupInfos == null ? null : new ArrayList<>(that.rowGroupInfos);
    this.endpointAffinities = that.endpointAffinities == null ? null : new ArrayList<>(that.endpointAffinities);
    this.mappings = that.mappings == null ? null : ArrayListMultimap.create(that.mappings);
//    this.parquetGroupScanStatistics = that.parquetGroupScanStatistics == null ? null : new ParquetGroupScanStatistics(that.parquetGroupScanStatistics);
//    this.fileSet = that.fileSet == null ? null : new HashSet<>(that.fileSet);
//    this.entries = that.entries == null ? null : new ArrayList<>(that.entries);
    this.readerConfig = that.readerConfig;
  }

  @Override
  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public List<ReadEntryWithPath> getEntries() {
    return rowGroups.keySet().stream()
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

    return rowGroups.entries().stream()
        .map(e -> {
          RowGroupMetadata rowGroupMetadata = e.getValue();
          RowGroupInfo rowGroupInfo = new RowGroupInfo(e.getKey().getLocation(),
              (long) rowGroupMetadata.getStatistic(() -> "start"),
              (long) rowGroupMetadata.getStatistic(() -> "length"),
              rowGroupMetadata.getRowGroupIndex(),
              (long) rowGroupMetadata.getStatistic(() -> "rowCount"));
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
    FilterPredicate filterPredicate = getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, false, tableMetadata.getFields());
    if (filterPredicate == null) {
      logger.debug("FilterPredicate cannot be built.");
      return null;
    }

    final Set<SchemaPath> schemaPathsInExpr =
      filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);

    RowGroupScanBuilder builder = getBuilder();

    filterTableMetadata(filterPredicate, schemaPathsInExpr, builder);

    filterPartitionMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);

    filterFileMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);

    filterRowGroupMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);

    if (builder.rowGroups.size() == rowGroups.size()) {
      // There is no reduction of files. Return the original groupScan.
      logger.debug("applyFilter() does not have any pruning!");
      matchAllRowGroups = builder.isMatchAllRowGroups();
      return null;
    } else if (!builder.isMatchAllRowGroups()
        && builder.getOverflowLevel() == MetadataLevel.NONE
        && (builder.getTableMetadata().isEmpty() || builder.getPartitions() == null
            || builder.getPartitions().isEmpty() || builder.getFiles() == null
            || builder.getFiles().isEmpty() || builder.rowGroups == null)) {
      if (rowGroups.size() == 1) {
        // For the case when group scan has single row group and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      logger.debug("All row groups have been filtered out. Add back one to get schema from scanner");
      builder.withMatching(false);
      PartitionMetadata nextPart = partitions.iterator().next();
      FileMetadata nextFile = files.get(nextPart).iterator().next();
      RowGroupMetadata nextRowGroup = rowGroups.get(nextFile).iterator().next();
      builder.withPartitions(Collections.singletonList(nextPart));
      builder.withPartitionFiles(Multimaps.newListMultimap(ImmutableMap.of(nextPart, Collections.singletonList(nextFile)), ArrayList::new));
      builder.withRowGroups(Multimaps.newListMultimap(ImmutableMap.of(nextFile, Collections.singletonList(nextRowGroup)), ArrayList::new));
    }

    logger.debug("applyFilter {} reduce row groups # from {} to {}",
      ExpressionStringBuilder.toString(filterExpr), rowGroups.size(), builder.rowGroups.size());

    return builder.build();
  }

  // narrows the return type
  protected abstract RowGroupScanBuilder getBuilder();

  protected void filterRowGroupMetadata(OptionManager optionManager, FilterPredicate filterPredicate,
                                        Set<SchemaPath> schemaPathsInExpr, RowGroupScanBuilder builder) {
    if (!builder.isMatchAllRowGroups() && builder.getPartitions() != null && builder.getPartitions().size() > 0) {
      Multimap<FileMetadata, RowGroupMetadata> prunedRowGroups;
      if (files.size() == builder.getFiles().size()) {
        // no partition pruning happened, no need to prune initial files list
        prunedRowGroups = rowGroups;
      } else {
        // prunes files to leave only files which are contained by pruned partitions
        prunedRowGroups = pruneRowGroupsForFiles(new ArrayList<>(builder.getFiles().values()));
      }

      // Stop files pruning for the case:
      //    -  # of files is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      if (prunedRowGroups.size() <= optionManager.getOption(
          PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {

        boolean matchAllRowGroupsLocal = matchAllRowGroups;

        Multimap<FileMetadata, RowGroupMetadata> filteredFiles = filterNextLevelMetadata(filterPredicate, schemaPathsInExpr, new ArrayList<>(builder.getFiles().values()), rowGroups);

        builder
          .withRowGroups(filteredFiles)
          .withMatching(matchAllRowGroups);

        matchAllRowGroups = matchAllRowGroupsLocal;
      } else {
        builder.withMatching(false)
          .withOverflow(MetadataLevel.FILE);
      }
    }
  }

  protected Multimap<FileMetadata, RowGroupMetadata> pruneRowGroupsForFiles(List<FileMetadata> filteredPartitionMetadata) {
    Multimap<FileMetadata, RowGroupMetadata> prunedFiles = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    rowGroups.entries().stream()
        .filter(entry -> filteredPartitionMetadata.contains(entry.getKey()))
        .forEach(entry -> prunedFiles.put(entry.getKey(), entry.getValue()));

    return prunedFiles;
  }

  protected Multimap<FileMetadata, RowGroupMetadata> filterRowGroups(LogicalExpression filterExpr,
      UdfUtilities udfUtilities, FunctionImplementationRegistry functionImplementationRegistry) {
    Multimap<FileMetadata, RowGroupMetadata> qualifiedRowGroups = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    Set<FileMetadata> fileMetadataList = rowGroups.keySet();

    if (fileMetadataList.size() > 0) {
      FilterPredicate filterPredicate = getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, true, tableMetadata.getFields());
      if (filterPredicate == null) {
        return null;
      }

      final Set<SchemaPath> schemaPathsInExpr =
        filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);

      for (Map.Entry<FileMetadata, ? extends Collection<RowGroupMetadata>> fileRowGroupEntry : rowGroups.asMap().entrySet()) {
        Collection<RowGroupMetadata> fileRowGroups = fileRowGroupEntry.getValue();
        // file has single row group and the same stats
        if (fileRowGroups.size() == 1) {
          qualifiedRowGroups.putAll(fileRowGroupEntry.getKey(), fileRowGroups);
          continue;
        }
        List<RowGroupMetadata> prunedRowGroups = filterAndGetMetadata(schemaPathsInExpr, fileRowGroups, filterPredicate);

        if (prunedRowGroups.size() > 0) {
          qualifiedRowGroups.putAll(fileRowGroupEntry.getKey(), prunedRowGroups);
        }
      }
    }
    return qualifiedRowGroups;
  }
  // filter push down methods block end

  // limit push down methods start
  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    // further optimization : minimize # of files chosen, or the affinity of files chosen.

    // Calculate number of rowGroups to read based on maxRecords and update
    // number of records to read for each of those rowGroups.
    int index = updateRowGroupInfo(maxRecords);

    Multimap<FileMetadata, RowGroupMetadata> qualifiedRowGroups = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    rowGroups.entries().stream()
        .limit(index)
        .forEach(e -> qualifiedRowGroups.put(e.getKey(), e.getValue()));

    // If there is no change in fileSet, no need to create new groupScan.
    if (qualifiedRowGroups.size() == rowGroups.size() ) {
      // There is no reduction of rowGroups. Return the original groupScan.
      logger.debug("applyLimit() does not apply!");
      return null;
    }

    logger.debug("applyLimit() reduce parquet file # from {} to {}", fileSet.size(), qualifiedRowGroups.keySet().size());

    try {
      AbstractParquetGroupScan newScan = cloneWithRowGroups(qualifiedRowGroups);
      newScan.updateRowGroupInfo(maxRecords);
      return newScan;
    } catch (IOException e) {
      logger.warn("Could not apply row count based prune due to Exception: {}", e);
      return null;
    }
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
    // TODO: make this method as deprecated and remoe its usage
    super.modifyFileSelection(selection);

    Multimap<FileMetadata, RowGroupMetadata> qualifiedRowGroups = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    rowGroups.entries().stream()
      .filter(entry -> fileSet.contains(entry.getKey().getLocation()))
      .forEach(entry -> qualifiedRowGroups.put(entry.getKey(), entry.getValue()));

    rowGroups = qualifiedRowGroups;
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

  // private methods block start
  /**
   * Based on maxRecords to read for the scan,
   * figure out how many rowGroups to read
   * and update number of records to read for each of them.
   *
   * @param maxRecords max records to read
   * @return total number of rowGroups to read
   */
  private int updateRowGroupInfo(int maxRecords) {
    long count = 0;
    int index = 0;
    for (RowGroupMetadata rowGroupInfo : rowGroups.values()) {
      long rowCount = (long) rowGroupInfo.getStatistic(() -> "rowCount");
      if (count + rowCount <= maxRecords) {
        count += rowCount;
        rowGroupAndNumsToRead.put(rowGroupInfo.getRowGroupIndex(), rowCount);
        index++;
        continue;
      } else if (count < maxRecords) {
        rowGroupAndNumsToRead.put(rowGroupInfo.getRowGroupIndex(), maxRecords - count);
        index++;
      }
      break;
    }
    return index;
  }
  // private methods block end

  protected static abstract class RowGroupScanBuilder extends GroupScanBuilder {
    protected Multimap<FileMetadata, RowGroupMetadata> rowGroups;

    public RowGroupScanBuilder withRowGroups(Multimap<FileMetadata, RowGroupMetadata> rowGroups) {
      this.rowGroups = rowGroups;
      return this;
    }
  }

}
