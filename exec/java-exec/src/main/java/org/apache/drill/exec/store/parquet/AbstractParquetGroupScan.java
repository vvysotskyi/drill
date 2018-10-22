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
  // whether all row groups of this group scan fully match the filter
  private boolean matchAllRowGroups = false;

  protected AbstractParquetGroupScan(String userName, List<SchemaPath> columns,
      List<ReadEntryWithPath> entries, LogicalExpression filter) {
    super(userName, columns, filter);
//    this.entries = entries;
  }

  // immutable copy constructor
  protected AbstractParquetGroupScan(AbstractParquetGroupScan that) {
    super(that.getUserName(), that.getColumns(), that.getFilter());
    this.rowGroups = that.rowGroups;
    this.files = that.files;
    this.tableMetadata = that.tableMetadata;
    this.fileSet = that.fileSet;
//    this.parquetTableMetadata = that.parquetTableMetadata;
//    this.rowGroupInfos = that.rowGroupInfos == null ? null : new ArrayList<>(that.rowGroupInfos);
    this.endpointAffinities = that.endpointAffinities == null ? null : new ArrayList<>(that.endpointAffinities);
    this.mappings = that.mappings == null ? null : ArrayListMultimap.create(that.mappings);
//    this.parquetGroupScanStatistics = that.parquetGroupScanStatistics == null ? null : new ParquetGroupScanStatistics(that.parquetGroupScanStatistics);
//    this.fileSet = that.fileSet == null ? null : new HashSet<>(that.fileSet);
//    this.entries = that.entries == null ? null : new ArrayList<>(that.entries);
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

  @JsonIgnore
  public boolean isMatchAllRowGroups() {
    return matchAllRowGroups;
  }

  @JsonIgnore
  @Override
  public Collection<String> getFiles() {
    return fileSet;
  }

  @Override
  public boolean hasFiles() {
    return true;
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
  public AbstractParquetGroupScan applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
      FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {

    if (!parquetTableMetadata.isRowGroupPrunable() ||
        rowGroupInfos.size() > optionManager.getOption(PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
      // Stop pruning for 2 cases:
      //    -  metadata does not have proper format to support row group level filter pruning,
      //    -  # of row files is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      return null;
    } else if (rowGroups.size() > optionManager.getOption(
      PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
      // Row group pruning cannot be applied, but file pruning is possible, so use it.
      // TODO: refactor code to fetch filtered files and create group scan with row groups from the files
      return super.applyFilter(filterExpr, udfUtilities, functionImplementationRegistry, optionManager);
    }

    List<FileMetadata> qualifiedFiles = filterFiles(filterExpr, udfUtilities, functionImplementationRegistry);
    if (qualifiedFiles == null) {
      return null;
    }
    Multimap<FileMetadata, RowGroupMetadata> qualifiedRowGroups = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    if (qualifiedFiles.size() == rowGroups.keySet().size()) {
      // There is no reduction of rowGroups. Return the original groupScan.
      logger.debug("applyFilter does not have file pruning!");

    final Set<SchemaPath> schemaPathsInExpr = filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);

    final List<RowGroupInfo> qualifiedRGs = new ArrayList<>(rowGroupInfos.size());

    ParquetFilterPredicate filterPredicate = getParquetFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, optionManager, true);

    if (filterPredicate == null) {
      return null;
    }

    boolean matchAllRowGroupsLocal = true;

    for (RowGroupInfo rowGroup : rowGroupInfos) {
      final ColumnExplorer columnExplorer = new ColumnExplorer(optionManager, columns);
      List<String> partitionValues = getPartitionValues(rowGroup);
      Map<String, String> implicitColValues = columnExplorer.populateImplicitColumns(rowGroup.getPath(), partitionValues, supportsFileImplicitColumns());

      Multimap<FileMetadata, RowGroupMetadata> filteredRowGroups = filterRowGroups(filterExpr, udfUtilities, functionImplementationRegistry);

      Map<SchemaPath, ColumnStatistics> columnStatisticsMap = statCollector.collectColStat(schemaPathsInExpr);

      ParquetFilterPredicate.RowsMatch match = ParquetRGFilterEvaluator.matches(filterPredicate,
          columnStatisticsMap, rowGroup.getRowCount(), parquetTableMetadata, rowGroup.getColumns(), schemaPathsInExpr);
      if (match == ParquetFilterPredicate.RowsMatch.NONE) {
        continue; // No row comply to the filter => drop the row group
      }
      // for the case when any of row groups partially matches the filter,
      // matchAllRowGroupsLocal should be set to false
      if (matchAllRowGroupsLocal) {
        matchAllRowGroupsLocal = match == ParquetFilterPredicate.RowsMatch.ALL;
      }

      qualifiedRGs.add(rowGroup);
    }

    if (qualifiedRGs.size() == rowGroupInfos.size()) {
      // There is no reduction of rowGroups. Return the original groupScan.
      logger.debug("applyFilter() does not have any pruning!");
      matchAllRowGroups = matchAllRowGroupsLocal;
      return null;
    } else if (qualifiedRGs.size() == 0) {
      if (rowGroupInfos.size() == 1) {
        // For the case when group scan has single row group and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      matchAllRowGroupsLocal = false;
      logger.debug("All row groups have been filtered out. Add back one to get schema from scanner.");
      RowGroupInfo rg = rowGroupInfos.iterator().next();
      qualifiedRGs.add(rg);
    }

    logger.debug("applyFilter {} reduce file # from {} to {} and / or row group number from {} to {}",
        ExpressionStringBuilder.toString(filterExpr), rowGroups.keySet().size(), qualifiedFiles.size(),
        rowGroups.size(), qualifiedRowGroups.size());
    try {
      AbstractParquetGroupScan cloneGroupScan = cloneWithRowGroupInfos(qualifiedRGs);
      cloneGroupScan.matchAllRowGroups = matchAllRowGroupsLocal;
      return cloneGroupScan;
    } catch (IOException e) {
      logger.warn("Could not apply filter prune due to Exception : {}", e);
      return null;
    }
  }

  /**
   * Returns parquet filter predicate built from specified {@code filterExpr}.
   *
   * @param filterExpr                     filter expression to build
   * @param udfUtilities                   udf utilities
   * @param functionImplementationRegistry context to find drill function holder
   * @param optionManager                  option manager
   * @param omitUnsupportedExprs           whether expressions which cannot be converted
   *                                       may be omitted from the resulting expression
   * @return parquet filter predicate
   */
  public ParquetFilterPredicate getParquetFilterPredicate(LogicalExpression filterExpr,
      UdfUtilities udfUtilities, FunctionImplementationRegistry functionImplementationRegistry,
      OptionManager optionManager, boolean omitUnsupportedExprs) {
    // used first row group to receive fields list
    assert rowGroupInfos.size() > 0 : "row groups count cannot be 0";
    RowGroupInfo rowGroup = rowGroupInfos.iterator().next();
    ColumnExplorer columnExplorer = new ColumnExplorer(optionManager, columns);

    Map<String, String> implicitColValues = columnExplorer.populateImplicitColumns(
        rowGroup.getPath(),
        getPartitionValues(rowGroup),
        supportsFileImplicitColumns());

    ParquetMetaStatCollector statCollector = new ParquetMetaStatCollector(
        parquetTableMetadata,
        rowGroup.getColumns(),
        implicitColValues);

    Set<SchemaPath> schemaPathsInExpr = filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);
    Map<SchemaPath, ColumnStatistics> columnStatisticsMap = statCollector.collectColStat(schemaPathsInExpr);

    ErrorCollector errorCollector = new ErrorCollectorImpl();
    LogicalExpression materializedFilter = ExpressionTreeMaterializer.materializeFilterExpr(
        filterExpr, columnStatisticsMap, errorCollector, functionImplementationRegistry);

    if (errorCollector.hasErrors()) {
      logger.error("{} error(s) encountered when materialize filter expression : {}",
          errorCollector.getErrorCount(), errorCollector.toErrorString());
      return null;
    }
    logger.debug("materializedFilter : {}", ExpressionStringBuilder.toString(materializedFilter));

    Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(materializedFilter);
    return ParquetFilterBuilder.buildParquetFilterPredicate(materializedFilter, constantBoundaries, udfUtilities, omitUnsupportedExprs);
  }

  protected Multimap<FileMetadata, RowGroupMetadata> filterRowGroups(LogicalExpression filterExpr,
      UdfUtilities udfUtilities, FunctionImplementationRegistry functionImplementationRegistry) {
    Multimap<FileMetadata, RowGroupMetadata> qualifiedRowGroups = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    Set<FileMetadata> fileMetadataList = rowGroups.keySet();

    if (fileMetadataList.size() > 0) {
      FilterPredicate filterPredicate = buildFilter(filterExpr, udfUtilities, functionImplementationRegistry, tableMetadata.getFields());
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
        List<RowGroupMetadata> prunedRowGroups = filterMetadata(schemaPathsInExpr, fileRowGroups, filterPredicate);

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

    if (parquetGroupScanStatistics.getRowCount() <= maxRecords) {
      logger.debug("limit push down does not apply, since total number of rows [{}] is less or equal to the required [{}].",
        parquetGroupScanStatistics.getRowCount(), maxRecords);
      return null;
    }

    // Calculate number of rowGroups to read based on maxRecords and update
    // number of records to read for each of those rowGroups.
    List<RowGroupInfo> qualifiedRowGroupInfos = new ArrayList<>(rowGroupInfos.size());
    int currentRowCount = 0;
    for (RowGroupInfo rowGroupInfo : rowGroupInfos) {
      long rowCount = rowGroupInfo.getRowCount();
      if (currentRowCount + rowCount <= maxRecords) {
        currentRowCount += rowCount;
        rowGroupInfo.setNumRecordsToRead(rowCount);
        qualifiedRowGroupInfos.add(rowGroupInfo);
        continue;
      } else if (currentRowCount < maxRecords) {
        rowGroupInfo.setNumRecordsToRead(maxRecords - currentRowCount);
        qualifiedRowGroupInfos.add(rowGroupInfo);
      }
      break;
    }

    Multimap<FileMetadata, RowGroupMetadata> qualifiedRowGroups = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    rowGroups.entries().stream()
        .limit(index)
        .forEach(e -> qualifiedRowGroups.put(e.getKey(), e.getValue()));

    // If there is no change in fileSet, no need to create new groupScan.
    if (rowGroupInfos.size() == qualifiedRowGroupInfos.size()) {
      // There is no reduction of rowGroups. Return the original groupScan.
      logger.debug("applyLimit() does not apply!");
      return null;
    }

    logger.debug("applyLimit() reduce parquet row groups # from {} to {}.", rowGroupInfos.size(), qualifiedRowGroupInfos.size());

    try {
      return cloneWithRowGroupInfos(qualifiedRowGroupInfos);
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
   * Clones current group scan with set of file paths from given row groups,
   * updates new scan with list of given row groups,
   * re-calculates statistics and endpoint affinities.
   *
   * @param rowGroupInfos list of row group infos
   * @return new parquet group scan
   */
  private AbstractParquetGroupScan cloneWithRowGroupInfos(List<RowGroupInfo> rowGroupInfos) throws IOException {
    Set<String> filePaths = rowGroupInfos.stream()
      .map(ReadEntryWithPath::getPath)
      .collect(Collectors.toSet()); // set keeps file names unique
    AbstractParquetGroupScan scan = cloneWithFileSelection(filePaths);
    scan.rowGroupInfos = rowGroupInfos;
    scan.parquetGroupScanStatistics.collect(scan.rowGroupInfos, scan.parquetTableMetadata);
    scan.endpointAffinities = AffinityCreator.getAffinityMap(scan.rowGroupInfos);
    return scan;
  }
  // private methods block end

}
