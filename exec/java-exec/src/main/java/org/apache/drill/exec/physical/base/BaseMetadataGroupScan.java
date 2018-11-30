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
package org.apache.drill.exec.physical.base;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.compile.sig.ConstantExpressionIdentifier;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.stat.ParquetFilterPredicate;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.parquet.ParquetRGFilterEvaluator;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.TableMetadata;
import org.apache.drill.metastore.TableStatistics;
import org.apache.drill.metastore.expr.FilterBuilder;
import org.apache.drill.metastore.expr.FilterPredicate;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimaps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseMetadataGroupScan extends AbstractFileGroupScan {
  protected TableMetadata tableMetadata;
  protected List<SchemaPath> columns;
  protected List<SchemaPath> partitionColumns = new ArrayList<>();
  protected LogicalExpression filter;
  protected List<PartitionMetadata> partitions;

  // TODO: what if there is no partitions? add default? use table as partition?
  protected Multimap<PartitionMetadata, FileMetadata> files = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);

//  protected MetadataBase.ParquetTableMetadataBase parquetTableMetadata;
//  private List<RowGroupInfo> rowGroupInfos;
//  protected List<ReadEntryWithPath> entries;
  // TODO: move to the child class
//  private List<CompleteFileWork> chunks;
//  private ListMultimap<Integer, CompleteFileWork> mappings;

  // set of the files to be handled
  protected Set<String> fileSet;

  // whether all row groups of this group scan fully match the filter
  protected boolean matchAllRowGroups = false;

  protected BaseMetadataGroupScan(String userName, List<SchemaPath> columns, LogicalExpression filter) {
    super(userName);
    this.columns = columns;
    this.filter = filter;
  }

//  public BaseMetadataGroupScan(String userName) {
//    super(userName);
//  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonIgnore
  public Map<SchemaPath, TypeProtos.MajorType> getColumnsMap() {
    return tableMetadata.getFields();
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

  @JsonIgnore
  public boolean isMatchAllRowGroups() {
    return matchAllRowGroups;
  }

  /**
   * Return column value count for the specified column.
   * If does not contain such column, return 0.
   * Is used when applying convert to direct scan rule.
   *
   * @param column column schema path
   * @return column value count
   */
  @Override
  public long getColumnValueCount(SchemaPath column) {
    return (long) tableMetadata.getColumnStats(column).getStatistic(() -> "rowCount");
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public ScanStats getScanStats() {
    int columnCount = columns == null ? 20 : columns.size();
    // TODO: add check for metadata availability
    long rowCount = (long) tableMetadata.getStatistic(() -> "rowCount");
    ScanStats scanStats = new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, rowCount, 1, rowCount * columnCount);
    logger.trace("Drill parquet scan statistics: {}", scanStats);
    return scanStats;
  }

  // filter push down methods block start
  @JsonProperty("filter")
  @Override
  public LogicalExpression getFilter() {
    return filter;
  }

  public void setFilter(LogicalExpression filter) {
    this.filter = filter;
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

    GroupScanBuilder builder = getBuilder();

    filterTableMetadata(filterPredicate, schemaPathsInExpr, builder);

    filterPartitionMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);

    filterFileMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);

    if (builder.files.size() == files.size()) {
      // There is no reduction of files. Return the original groupScan.
      logger.debug("applyFilter() does not have any pruning!");
      matchAllRowGroups = builder.matchAllRowGroups;
      return null;
    } else if (!builder.matchAllRowGroups
        && builder.overflowLevel == MetadataLevel.NONE
        && (builder.tableMetadata.isEmpty() || builder.partitions == null
            || builder.partitions.isEmpty() || builder.files == null)) {
      if (files.size() == 1) {
        // For the case when group scan has single row group and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      logger.debug("All files have been filtered out. Add back one to get schema from scanner");
      builder.withMatching(false);
      PartitionMetadata nextPart = partitions.iterator().next();
      FileMetadata nextFile = files.get(nextPart).iterator().next();
      builder.withPartitions(Collections.singletonList(nextPart));
      builder.withPartitionFiles(Multimaps.newListMultimap(ImmutableMap.of(nextPart, Collections.singletonList(nextFile)), ArrayList::new));
    }

    logger.debug("applyFilter() {} reduce file # from {} to {}",
        ExpressionStringBuilder.toString(filterExpr), files.size(), builder.files.size());

    return builder.build();
  }

  protected abstract GroupScanBuilder getBuilder();

  protected Multimap<PartitionMetadata, FileMetadata> pruneFilesForPartitions(List<PartitionMetadata> filteredPartitionMetadata) {
    Multimap<PartitionMetadata, FileMetadata> prunedFiles = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    files.entries().stream()
        .filter(entry -> filteredPartitionMetadata.contains(entry.getKey()))
        .forEach(entry -> prunedFiles.put(entry.getKey(), entry.getValue()));
    return prunedFiles;
  }

  protected void filterFileMetadata(OptionManager optionManager, FilterPredicate filterPredicate,
                                    Set<SchemaPath> schemaPathsInExpr, GroupScanBuilder builder) {
    if (!builder.matchAllRowGroups && builder.partitions != null && builder.partitions.size() > 0) {
      Multimap<PartitionMetadata, FileMetadata> prunedFiles;
      if (partitions.size() == builder.partitions.size()) {
        // no partition pruning happened, no need to prune initial files list
        prunedFiles = files;
      } else {
        // prunes files to leave only files which are contained by pruned partitions
        prunedFiles = pruneFilesForPartitions(builder.partitions);
      }

      // Stop files pruning for the case:
      //    -  # of files is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      if (prunedFiles.size() <= optionManager.getOption(
        PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {

        boolean matchAllRowGroupsLocal = matchAllRowGroups;

        Multimap<PartitionMetadata, FileMetadata> filteredFiles = filterNextLevelMetadata(filterPredicate, schemaPathsInExpr, builder.partitions, files);

        builder
            .withPartitionFiles(filteredFiles)
            .withMatching(matchAllRowGroups);

        matchAllRowGroups = matchAllRowGroupsLocal;
      } else {
        builder.withMatching(false)
          .withOverflow(MetadataLevel.FILE);
      }
    }
  }

  protected void filterPartitionMetadata(OptionManager optionManager, FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr, GroupScanBuilder builder) {
    if (builder.tableMetadata.size() > 0 && !builder.matchAllRowGroups) {
      if (partitions.size() <= optionManager.getOption(
          PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
        boolean matchAllRowGroupsLocal = matchAllRowGroups;
        List<PartitionMetadata> filteredPartitionMetadata = filterAndGetMetadata(schemaPathsInExpr, partitions, filterPredicate);
        builder.withPartitions(filteredPartitionMetadata)
          .withMatching(matchAllRowGroups);
        matchAllRowGroups = matchAllRowGroupsLocal;
      } else {
        builder.withMatching(false)
            .withOverflow(MetadataLevel.PARTITION);
      }
    }
  }

  protected void filterTableMetadata(FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr, GroupScanBuilder builder) {
    boolean matchAllRowGroupsLocal = matchAllRowGroups;
    // Filters table metadata. If resulting list is empty, should be used single minimum entity of metadata.
    // If table matches fully, nothing is pruned and pruning of underlying metadata is stopped.
    List<TableMetadata> filteredTableMetadata =
        filterAndGetMetadata(schemaPathsInExpr, Collections.singletonList(this.tableMetadata), filterPredicate);

    builder.withTable(filteredTableMetadata);
    builder.withMatching(matchAllRowGroups);
    matchAllRowGroups = matchAllRowGroupsLocal;
  }

  protected <B extends BaseMetadata, U extends BaseMetadata> Multimap<B, U> filterNextLevelMetadata(
      FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr,
      List<B> filteredPartitionMetadata, Multimap<B, U> metadataToFilter) {
    Multimap<B, U> filteredFiles = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    for (B partMet : filteredPartitionMetadata) {
      Collection<U> fileMetadata = metadataToFilter.get(partMet);

      List<U> prunedFiles = filterAndGetMetadata(schemaPathsInExpr, fileMetadata, filterPredicate);

      if (prunedFiles.size() > 0) {
        filteredFiles.putAll(partMet, prunedFiles);
      }
    }
    return filteredFiles;
  }
  // filter push down methods block end

//  protected List<BaseMetadata> filterTableAndFiles(LogicalExpression filterExpr, UdfUtilities udfUtilities,
//                                                   FunctionImplementationRegistry functionImplementationRegistry) {
//    List<BaseMetadata> qualifiedMetadata = new ArrayList<>();
//    if (files.size() > 0) {
//      FilterPredicate filterPredicate = getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, false, tableMetadata.getFields());
//      if (filterPredicate == null) {
//        return null;
//      }
//
//      final Set<SchemaPath> schemaPathsInExpr =
//        filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);
//
//      List<TableMetadata> filteredTableMetadata =
//        filterAndGetMetadata(schemaPathsInExpr, Collections.singletonList(this.tableMetadata), filterPredicate);
//
//      if (filteredTableMetadata.isEmpty()) {
//        logger.debug("All rows have been filtered out during applying the filter to the table itself.");
//      } else {
//        qualifiedMetadata.addAll(filterAndGetMetadata(schemaPathsInExpr, files, filterPredicate));
//      }
//    }
//    return qualifiedMetadata;
//  }

  protected <T extends BaseMetadata> List<T> filterAndGetMetadata(Set<SchemaPath> schemaPathsInExpr,
                                                                  Iterable<T> metadataList,
                                                                  FilterPredicate filterPredicate) {
    List<T> qualifiedFiles = new ArrayList<>();

    for (T metadata : metadataList) {
      // TODO: decide where implicit + partition columns should be handled: either they should be present in
      // file metadata or they should be populated in this method and passed with other columns.

      ParquetFilterPredicate.RowsMatch match = ParquetRGFilterEvaluator.matches(filterPredicate,
          metadata.getColumnStatistics(), (long) metadata.getStatistic(TableStatistics.ROW_COUNT),
          metadata.getFields(), schemaPathsInExpr);
      if (match == ParquetFilterPredicate.RowsMatch.NONE) {
        continue; // No file comply to the filter => drop the row group
      }
      if (matchAllRowGroups) {
        matchAllRowGroups = match == ParquetFilterPredicate.RowsMatch.ALL;
      }
      qualifiedFiles.add(metadata);
    }
    return qualifiedFiles;
  }

  /**
   * Returns parquet filter predicate built from specified {@code filterExpr}.
   *
   * @param filterExpr                     filter expression to build
   * @param udfUtilities                   udf utilities
   * @param functionImplementationRegistry context to find drill function holder
   * @param omitUnsupportedExprs           whether expressions which cannot be converted
   *                                       may be omitted from the resulting expression
   * @return parquet filter predicate
   */
  public FilterPredicate getFilterPredicate(LogicalExpression filterExpr,
                                            UdfUtilities udfUtilities, FunctionImplementationRegistry functionImplementationRegistry,
                                            boolean omitUnsupportedExprs, Map<SchemaPath, TypeProtos.MajorType> types) {

    ErrorCollector errorCollector = new ErrorCollectorImpl();
    LogicalExpression materializedFilter = ExpressionTreeMaterializer.materializeFilterExpr(
      filterExpr, types, errorCollector, functionImplementationRegistry);

    if (errorCollector.hasErrors()) {
      logger.error("{} error(s) encountered when materialize filter expression : {}",
        errorCollector.getErrorCount(), errorCollector.toErrorString());
      return null;
    }
    logger.debug("materializedFilter : {}", ExpressionStringBuilder.toString(materializedFilter));

    Set<LogicalExpression> constantBoundaries = ConstantExpressionIdentifier.getConstantExpressionSet(materializedFilter);
    return FilterBuilder.buildFilterPredicate(materializedFilter, constantBoundaries, udfUtilities, omitUnsupportedExprs);
  }

  private boolean containsImplicitCol(Set<SchemaPath> schemaPaths, OptionManager optionManager) {
    Set<String> implicitColNames = ColumnExplorer.initImplicitFileColumns(optionManager).keySet();
    for (SchemaPath schemaPath : schemaPaths) {
      if (ColumnExplorer.isPartitionColumn(optionManager, schemaPath) || implicitColNames.contains(schemaPath.getRootSegmentPath())) {
        return true;
      }
    }
    return false;
  }

  // limit push down methods start
  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    // further optimization : minimize # of files chosen, or the affinity of files chosen.

    long tableRowCount = (long) tableMetadata.getStatistic(() -> "rowCount");
    if (tableRowCount <= maxRecords) {
      logger.debug("limit push down does not apply, since total number of rows [{}] is less or equal to the required [{}].",
        tableRowCount, maxRecords);
      return null;
    }

    // Calculate number of files to read based on maxRecords and update
    // number of records to read for each of those rowGroups.
    Multimap<PartitionMetadata, FileMetadata> qualifiedFiles = Multimaps.newListMultimap(new HashMap<>(), ArrayList::new);
    int currentRowCount = 0;
    for (Map.Entry<PartitionMetadata, FileMetadata> rowGroupInfo : files.entries()) {
      long rowCount = (long) rowGroupInfo.getKey().getStatistic(() -> "rowCount");
      if (currentRowCount + rowCount <= maxRecords) {
        currentRowCount += rowCount;
//        rowGroupInfo.setNumRecordsToRead(rowCount);
        qualifiedFiles.put(rowGroupInfo.getKey(), rowGroupInfo.getValue());
        continue;
      } else if (currentRowCount < maxRecords) {
//        rowGroupInfo.setNumRecordsToRead(maxRecords - currentRowCount);
        qualifiedFiles.put(rowGroupInfo.getKey(), rowGroupInfo.getValue());
      }
      break;
    }

    if (files.size() == qualifiedFiles.size()) {
      logger.debug("limit push down does not apply, since number of row groups was not reduced.");
      return null;
    }

    logger.debug("applyLimit() reduce files # from {} to {}.", files.size(), qualifiedFiles.size());

    return getBuilder()
        .withPartitions(new ArrayList<>(qualifiedFiles.keys()))
        .withPartitionFiles(qualifiedFiles)
        .build();
  }
  // limit push down methods end

  // partition pruning methods start
  @Override
  public List<SchemaPath> getPartitionColumns() {
    return partitionColumns;
  }

  @JsonIgnore
  public TypeProtos.MajorType getTypeForColumn(SchemaPath schemaPath) {
    return tableMetadata.getField(schemaPath);
  }

  @JsonIgnore
  public <T> T getPartitionValue(String path, SchemaPath column, Class<T> clazz) {
    // TODO: add path-to-file metadata map to avoid filtering
    return files.values().stream()
        .filter(file -> file.getLocation().equals(path))
        .findAny()
        .map(metadata -> clazz.cast(metadata.getStatistic(() -> "minValue")))
        .orElse(null);
//    parquetGroupScanStatistics.getPartitionValue(path, column));
  }

  @JsonIgnore
  public Set<String> getFileSet() {
    return fileSet;
  }
  // partition pruning methods end

  // helper method used for partition pruning and filter push down
  @Override
  public void modifyFileSelection(FileSelection selection) {
    List<String> files = selection.getFiles();
    fileSet = new HashSet<>(files);
  }

  // protected methods block
  protected void init() throws IOException {
    initInternal();

    if (fileSet == null) {
      fileSet = files.values().stream()
        .map(FileMetadata::getLocation)
        .collect(Collectors.toSet());
    }
  }

  protected String getFilterString() {
    return filter == null || filter.equals(ValueExpressions.BooleanExpression.TRUE) ?
      "" : ExpressionStringBuilder.toString(this.filter);
  }

  // abstract methods block start
  protected abstract void initInternal() throws IOException;

  //  protected abstract BaseMetadataGroupScan cloneWithFileSelection(Collection<String> filePaths) throws IOException;
//  protected abstract BaseMetadataGroupScan cloneWithFileSet(Collection<FileMetadata> files) throws IOException;

//  protected abstract BaseMetadataGroupScan cloneWith(List<PartitionMetadata> partitions, Multimap<PartitionMetadata, FileMetadata> files) throws IOException;

  protected abstract boolean supportsFileImplicitColumns();
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
//  private BaseMetadataGroupScan cloneWithRowGroupInfos(List<RowGroupInfo> rowGroupInfos) throws IOException {
//    Set<String> filePaths = rowGroupInfos.stream()
//      .map(ReadEntryWithPath::getPath)
//      .collect(Collectors.toSet()); // set keeps file names unique
//    AbstractParquetGroupScan scan = cloneWithFileSelection(filePaths);
//    scan.rowGroupInfos = rowGroupInfos;
//    scan.parquetGroupScanStatistics.collect(scan.rowGroupInfos, scan.parquetTableMetadata);
//    scan.endpointAffinities = AffinityCreator.getAffinityMap(scan.rowGroupInfos);
//    return scan;
//  }

  protected abstract static class GroupScanBuilder {
    protected boolean matchAllRowGroups = false;

    protected List<TableMetadata> tableMetadata;
    protected List<PartitionMetadata> partitions;
    protected Multimap<PartitionMetadata, FileMetadata> files;
    protected MetadataLevel overflowLevel = MetadataLevel.NONE;

    public abstract BaseMetadataGroupScan build();

    public GroupScanBuilder withTable(List<TableMetadata> filteredTableMetadata) {
      this.tableMetadata = filteredTableMetadata;
      return this;
    }

    public GroupScanBuilder withPartitions(List<PartitionMetadata> partitions) {
      this.partitions = partitions;
      return this;
    }

    public GroupScanBuilder withPartitionFiles(Multimap<PartitionMetadata, FileMetadata> files) {
      this.files = files;
      return this;
    }

    public GroupScanBuilder withMatching(boolean matchAllRowGroups) {
      this.matchAllRowGroups = matchAllRowGroups;
      return this;
    }

    public GroupScanBuilder withOverflow(MetadataLevel overflowLevel) {
      this.overflowLevel = overflowLevel;
      return this;
    }

    public boolean isMatchAllRowGroups() {
      return matchAllRowGroups;
    }

    public List<TableMetadata> getTableMetadata() {
      return tableMetadata;
    }

    public List<PartitionMetadata> getPartitions() {
      return partitions;
    }

    public Multimap<PartitionMetadata, FileMetadata> getFiles() {
      return files;
    }

    public MetadataLevel getOverflowLevel() {
      return overflowLevel;
    }
  }

  public enum MetadataLevel {
    TABLE, PARTITION, FILE, ROW_GROUP, NONE
  }
}
