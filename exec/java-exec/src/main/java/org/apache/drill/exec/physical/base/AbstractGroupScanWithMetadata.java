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
import org.apache.drill.common.types.Types;
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
import org.apache.drill.exec.expr.stat.RowsMatch;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.parquet.FilterEvaluatorUtils;
import org.apache.drill.exec.store.parquet.ParquetTableMetadataUtils;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.ColumnStatistic;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.LocationProvider;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.TableMetadata;
import org.apache.drill.metastore.TableStatistics;
import org.apache.drill.metastore.expr.FilterBuilder;
import org.apache.drill.metastore.expr.FilterPredicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// TODO: note that all these group scan operators are JSON serializable. Check is it still correct.
public abstract class AbstractGroupScanWithMetadata extends AbstractFileGroupScan {

  protected TableMetadataProvider metadataProvider;

  // table metadata info
  protected TableMetadata tableMetadata;

  // partition metadata info: mixed partition values for all partition keys in the same list
  protected List<PartitionMetadata> partitions;

  protected List<SchemaPath> partitionColumns;
  protected LogicalExpression filter;
  protected List<SchemaPath> columns;

  protected List<FileMetadata> files;

  // set of the files to be handled
  protected Set<String> fileSet;

  // whether all row groups of this group scan fully match the filter
  protected boolean matchAllRowGroups = false;

  protected AbstractGroupScanWithMetadata(String userName, List<SchemaPath> columns, LogicalExpression filter) {
    super(userName);
    this.columns = columns;
    this.filter = filter;
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
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
    // TODO: may return wrong results for the case when the same table metadata is used after
    //  partitions/files/row groups pruning or if table metadata is unavailable
    if (tableMetadata != null) {
      long tableRowCount = (long) TableStatistics.ROW_COUNT.getValue(tableMetadata);
      ColumnStatistic columnStats = tableMetadata.getColumnStats(column);
      long colNulls;
      if (columnStats != null) {
        colNulls = (long) columnStats.getStatistic(ColumnStatisticsKind.NULLS_COUNT);
      } else {
        colNulls = GroupScan.NO_COLUMN_STATS;
      }
      return GroupScan.NO_COLUMN_STATS == tableRowCount
          || GroupScan.NO_COLUMN_STATS == colNulls
          ? GroupScan.NO_COLUMN_STATS : tableRowCount - colNulls;
    } else if (files != null && !files.isEmpty()) {
      // TODO: collect from files?
    }
    return GroupScan.NO_COLUMN_STATS;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public ScanStats getScanStats() {
    int columnCount = columns == null ? 20 : columns.size();
    long rowCount = 0;
    // TODO: replace with table metadata when its row count is fixed
    if (files != null) {
      for (FileMetadata file : files) {
        rowCount += (long) file.getStatistic(TableStatistics.ROW_COUNT);
      }
    }

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
  public AbstractGroupScanWithMetadata applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
                                                   FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {

    // Builds filter for pruning. If filter cannot be built, null should be returned.
    FilterPredicate filterPredicate = getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, optionManager, true);
    if (filterPredicate == null) {
      logger.debug("FilterPredicate cannot be built.");
      return null;
    }

    final Set<SchemaPath> schemaPathsInExpr =
        filterExpr.accept(new FilterEvaluatorUtils.FieldReferenceFinder(), null);

    GroupScanWithMetadataBuilder builder = getFiltered(optionManager, filterPredicate, schemaPathsInExpr);

    if (files != null) {
      if (builder.getFiles() != null && files.size() == builder.getFiles().size()) {
        // There is no reduction of files, but filter may be omitted.
        logger.debug("applyFilter() does not have any pruning since GroupScan fully matches filter");
        matchAllRowGroups = builder.isMatchAllRowGroups();
        return null;
      }
    } else if (partitions != null) {
      // for the case when files metadata wasn't created, check partition metadata
      if (builder.getPartitions() != null && partitions.size() == builder.getPartitions().size()) {
        // There is no reduction of partitions, but filter may be omitted.
        logger.debug("applyFilter() does not have any pruning since GroupScan fully matches filter");
        matchAllRowGroups = builder.isMatchAllRowGroups();
        return null;
      }
    } else if (tableMetadata != null) {
      // There is no reduction of files, but filter may be omitted.
      logger.debug("applyFilter() does not have any pruning since GroupScan fully matches filter");
      matchAllRowGroups = builder.isMatchAllRowGroups();
      return null;
    }

    if (!builder.isMatchAllRowGroups()
      // filter returns empty result using table metadata
      && (((builder.getTableMetadata() == null || builder.getTableMetadata().isEmpty()) && tableMetadata != null)
          // all partitions pruned if partition metadata is available
          || unchangedMetadata(partitions, builder.getPartitions())
          // all files are pruned if file metadata is available
          || unchangedMetadata(files, builder.getFiles()))) {
      if (files.size() == 1) {
        // For the case when group scan has single file and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      logger.debug("All files have been filtered out. Add back one to get schema from scanner");
      builder.withTable(tableMetadata != null ? Collections.singletonList(tableMetadata) : Collections.emptyList())
          .withPartitions(getNextOrEmpty(partitions))
          .withFiles(getNextOrEmpty(files))
          .withMatching(false);
    }

//    logger.debug("applyFilter() {} reduce file # from {} to {}",
//        ExpressionStringBuilder.toString(filterExpr), files.size(), builder.getFiles().size());

    return builder.build();
  }

  protected static <T> boolean unchangedMetadata(List<T> partitions, List<T> newPartitions) {
    return (newPartitions == null || newPartitions.isEmpty()) && partitions != null && !partitions.isEmpty();
  }

  /** Javadoc javadoc javadoc.
   * @param inputList param javadoc
   * @param <T> type javadoc
   * @return return javadoc
   */
  protected <T> List<T> getNextOrEmpty(List<T> inputList) {
    return inputList != null && !inputList.isEmpty() ? Collections.singletonList(inputList.iterator().next()) : Collections.emptyList();
  }

  protected GroupScanWithMetadataBuilder getFiltered(OptionManager optionManager, FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr) {
    GroupScanWithMetadataBuilder builder = getBuilder();

    if (tableMetadata != null) {
      filterTableMetadata(filterPredicate, schemaPathsInExpr, builder);
    }

    if (partitions != null) {
      filterPartitionMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);
    }

    if (files != null) {
      filterFileMetadata(optionManager, filterPredicate, schemaPathsInExpr, builder);
    }
    return builder;
  }

  protected abstract GroupScanWithMetadataBuilder getBuilder();

  protected <T extends BaseMetadata & LocationProvider> List<T> pruneForPartitions(List<T> metadataToPrune, List<PartitionMetadata> filteredPartitionMetadata) {
    List<T> prunedFiles = new ArrayList<>();
    if (metadataToPrune != null) {
      for (T file : metadataToPrune) {
        for (PartitionMetadata filteredPartition : filteredPartitionMetadata) {
          if (filteredPartition.getLocations().contains(file.getLocation())) {
            prunedFiles.add(file);
            break;
          }
        }
      }
    }

    return prunedFiles;
  }

  protected void filterFileMetadata(OptionManager optionManager, FilterPredicate filterPredicate,
                                    Set<SchemaPath> schemaPathsInExpr, GroupScanWithMetadataBuilder builder) {
    List<FileMetadata> prunedFiles;
    if (partitions == null || partitions.isEmpty() || partitions.size() == builder.partitions.size()) {
      // no partition pruning happened, no need to prune initial files list
      prunedFiles = files;
    } else {
      // prunes files to leave only files which are contained by pruned partitions
      prunedFiles = pruneForPartitions(files, builder.partitions);
    }

    if (builder.matchAllRowGroups) {
      builder.withFiles(prunedFiles);
      return;
    }

    // Stop files pruning for the case:
    //    -  # of files is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
    if (prunedFiles.size() <= optionManager.getOption(
      PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {

      boolean matchAllRowGroupsLocal = matchAllRowGroups;
      matchAllRowGroups = true;
      List<FileMetadata> filteredFiles = filterAndGetMetadata(schemaPathsInExpr, prunedFiles, filterPredicate, optionManager);

      builder.withFiles(filteredFiles)
          .withMatching(matchAllRowGroups);

      matchAllRowGroups = matchAllRowGroupsLocal;
    } else {
      builder.withMatching(false)
          .withFiles(prunedFiles)
          .withOverflow(MetadataLevel.FILE);
    }
  }

  protected void filterPartitionMetadata(OptionManager optionManager, FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr, GroupScanWithMetadataBuilder builder) {
    if (!builder.matchAllRowGroups) {
      if (!partitions.isEmpty()) {
        if (partitions.size() <= optionManager.getOption(
          PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
          boolean matchAllRowGroupsLocal = matchAllRowGroups;
          matchAllRowGroups = true;
          List<PartitionMetadata> filteredPartitionMetadata = filterAndGetMetadata(schemaPathsInExpr, partitions, filterPredicate, optionManager);
          builder.withPartitions(filteredPartitionMetadata)
              .withMatching(matchAllRowGroups);
          matchAllRowGroups = matchAllRowGroupsLocal;
        } else {
          builder.withMatching(false)
            .withOverflow(MetadataLevel.PARTITION);
        }
      }
    } else {
      builder.withPartitions(partitions);
    }
  }

  protected void filterTableMetadata(FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr, GroupScanWithMetadataBuilder builder) {
    boolean matchAllRowGroupsLocal = matchAllRowGroups;
    // Filters table metadata. If resulting list is empty, should be used single minimum entity of metadata.
    // If table matches fully, nothing is pruned and pruning of underlying metadata is stopped.
    matchAllRowGroups = true;
    List<TableMetadata> filteredTableMetadata =
        filterAndGetMetadata(schemaPathsInExpr, Collections.singletonList(this.tableMetadata), filterPredicate, null);

    builder.withTable(filteredTableMetadata);
    builder.withMatching(matchAllRowGroups);
    matchAllRowGroups = matchAllRowGroupsLocal;
  }

  protected <T extends BaseMetadata> List<T> filterAndGetMetadata(Set<SchemaPath> schemaPathsInExpr,
                                                                  Iterable<T> metadataList,
                                                                  FilterPredicate filterPredicate,
                                                                  OptionManager optionManager) {
    List<T> qualifiedFiles = new ArrayList<>();

    for (T metadata : metadataList) {
      Map<SchemaPath, ColumnStatistic> columnStatistics = metadata.getColumnStatistics();

      // adds partition (dir) column statistics if it may be used during filter evaluation
      if (metadata instanceof LocationProvider && optionManager != null) {
        LocationProvider locationProvider = (LocationProvider) metadata;
        columnStatistics = ParquetTableMetadataUtils.addImplicitColumnsStatistic(columnStatistics,
            columns, getPartitionValues(locationProvider), optionManager, locationProvider.getLocation(), supportsFileImplicitColumns());
      }

      RowsMatch match = FilterEvaluatorUtils.matches(filterPredicate,
          columnStatistics, (long) metadata.getStatistic(TableStatistics.ROW_COUNT),
          metadata.getSchema(), schemaPathsInExpr);
      if (match == RowsMatch.NONE) {
        continue; // No file comply to the filter => drop the file
      }
      if (matchAllRowGroups) {
        matchAllRowGroups = match == RowsMatch.ALL;
      }
      qualifiedFiles.add(metadata);
    }
    if (qualifiedFiles.isEmpty()) {
      matchAllRowGroups = false;
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
                                            OptionManager optionManager, boolean omitUnsupportedExprs) {

    TupleMetadata types = getColumnMetadata();
    if (types == null) {
      throw new UnsupportedOperationException("At least one schema source should be available.");
    }

    Set<SchemaPath> schemaPathsInExpr = filterExpr.accept(new FilterEvaluatorUtils.FieldReferenceFinder(), null);

    // adds implicit or partition columns if they weren't added before.
    if (supportsFileImplicitColumns()) {
      for (SchemaPath schemaPath : schemaPathsInExpr) {
        if (isImplicitOrPartCol(schemaPath, optionManager) && SchemaPathUtils.getColumnMetadata(schemaPath, types) == null) {
          types.add(MaterializedField.create(schemaPath.getRootSegmentPath(), Types.required(TypeProtos.MinorType.VARCHAR)));
        }
      }
    }

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

  protected TupleMetadata getColumnMetadata() {
    if (tableMetadata != null) {
      return tableMetadata.getSchema().copy();
    } else {
      if (partitions != null && !partitions.isEmpty()) {
        return partitions.iterator().next().getSchema();
      } else {
        if (files != null && !files.isEmpty()) {
          return files.iterator().next().getSchema();
        }
      }
    }
    return null;
  }

  // limit push down methods start
  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    GroupScanWithMetadataBuilder builder = limitFiles(maxRecords);
    if (tableMetadata != null && (builder.getTableMetadata() == null || builder.getTableMetadata().isEmpty())) {
      logger.debug("limit push down does not apply, since table has less rows.");
      return null;
    }
    if (files != null && !files.isEmpty() && (builder.getFiles() == null || files.size() == builder.getFiles().size() || builder.getFiles().isEmpty())) {
      logger.debug("limit push down does not apply, since number of files was not reduced.");
      return null;
    }
    if (partitions != null && !partitions.isEmpty() && (builder.getPartitions() == null || partitions.size() == builder.getPartitions().size() || builder.getPartitions().isEmpty())) {
      logger.debug("limit push down does not apply, since number of partitions was not reduced.");
      return null;
    }
    return builder.build();
  }

  protected GroupScanWithMetadataBuilder limitFiles(int maxRecords) {
    GroupScanWithMetadataBuilder builder = getBuilder();
    // further optimization : minimize # of files chosen, or the affinity of files chosen.

    if (tableMetadata != null) {
      long tableRowCount = (long) tableMetadata.getStatistic(TableStatistics.ROW_COUNT);
      if (tableRowCount <= maxRecords) {
        logger.debug("limit push down does not apply, since total number of rows [{}] is less or equal to the required [{}].",
            tableRowCount, maxRecords);
        return builder;
      }
    }

    List<PartitionMetadata> qualifiedPartitions = limitMetadata(partitions != null ? partitions : Collections.emptyList(), maxRecords);

    List<FileMetadata> partFiles = partitions != null && !partitions.isEmpty() ? pruneForPartitions(files, qualifiedPartitions) : files;

    // Calculate number of files to read based on maxRecords and update
    // number of records to read for each of those rowGroups.
    List<FileMetadata> qualifiedFiles = limitMetadata(partFiles != null ? partFiles : Collections.emptyList(), maxRecords);

    return builder
        .withTable(tableMetadata != null ? Collections.singletonList(tableMetadata) : Collections.emptyList())
        .withPartitions(qualifiedPartitions)
        .withFiles(qualifiedFiles);
  }

  protected <T extends BaseMetadata> List<T> limitMetadata(List<T> metadataList, int maxRecords) {
    List<T> qualifiedMetadata = new ArrayList<>();
    int currentRowCount = 0;
    for (T metadata : metadataList) {
      long rowCount = (long) TableStatistics.ROW_COUNT.getValue(metadata);
      if (currentRowCount + rowCount <= maxRecords) {
        currentRowCount += rowCount;
        qualifiedMetadata.add(metadata);
        continue;
      } else if (currentRowCount < maxRecords) {
        qualifiedMetadata.add(metadata);
      }
      break;
    }
    return qualifiedMetadata;
  }
  // limit push down methods end

  // partition pruning methods start
  @Override
  public List<SchemaPath> getPartitionColumns() {
    return partitionColumns != null ? partitionColumns : new ArrayList<>();
  }

  @JsonIgnore
  public TypeProtos.MajorType getTypeForColumn(SchemaPath schemaPath) {
    ColumnMetadata columnMetadata = SchemaPathUtils.getColumnMetadata(schemaPath, tableMetadata.getSchema());
    return columnMetadata != null ? columnMetadata.majorType() : null;
  }

  @JsonIgnore
  public <T> T getPartitionValue(String path, SchemaPath column, Class<T> clazz) {
    // TODO: add path-to-file metadata map to avoid filtering
    if (partitions != null) {
      return partitions.stream()
          .filter(partition -> partition.getColumn().equals(column) && partition.getLocations().contains(path))
          .findAny()
          .map(metadata -> clazz.cast(metadata.getColumnStatistics().get(column).getStatistic(ColumnStatisticsKind.MAX_VALUE)))
          .orElse(null);
    }
    return null;
  }

  @JsonIgnore
  public Set<String> getFileSet() {
    return fileSet;
  }
  // partition pruning methods end
// above and below comments are not useful anymore
  // helper method used for partition pruning and filter push down
  @Override
  public void modifyFileSelection(FileSelection selection) {
    fileSet = new HashSet<>(selection.getFiles());
  }

  // protected methods block
  protected void init() throws IOException {
    if (fileSet == null && files != null) {
      fileSet = files.stream()
          .map(FileMetadata::getLocation)
          .collect(Collectors.toSet());
    }
  }

  protected String getFilterString() {
    return filter == null || filter.equals(ValueExpressions.BooleanExpression.TRUE) ?
      "" : ExpressionStringBuilder.toString(this.filter);
  }

  protected abstract boolean supportsFileImplicitColumns();
  protected abstract List<String> getPartitionValues(LocationProvider rowGroupInfo);

  protected boolean isImplicitOrPartCol(SchemaPath schemaPath, OptionManager optionManager) {
    Set<String> implicitColNames = ColumnExplorer.initImplicitFileColumns(optionManager).keySet();
    return ColumnExplorer.isPartitionColumn(optionManager, schemaPath) || implicitColNames.contains(schemaPath.getRootSegmentPath());
  }

  protected List<PartitionMetadata> getPartitions() {
    return metadataProvider.getPartitionsMetadata();
  }

  protected abstract static class GroupScanWithMetadataBuilder {
    protected boolean matchAllRowGroups = false;

    protected List<TableMetadata> tableMetadata;
    protected List<PartitionMetadata> partitions;
    protected List<FileMetadata> files;
    protected MetadataLevel overflowLevel = MetadataLevel.NONE;

    public abstract AbstractGroupScanWithMetadata build();

    public GroupScanWithMetadataBuilder withTable(List<TableMetadata> filteredTableMetadata) {
      this.tableMetadata = filteredTableMetadata;
      return this;
    }

    public GroupScanWithMetadataBuilder withPartitions(List<PartitionMetadata> partitions) {
      this.partitions = partitions;
      return this;
    }

    public GroupScanWithMetadataBuilder withFiles(List<FileMetadata> files) {
      this.files = files;
      return this;
    }

    public GroupScanWithMetadataBuilder withMatching(boolean matchAllRowGroups) {
      this.matchAllRowGroups = matchAllRowGroups;
      return this;
    }

    public GroupScanWithMetadataBuilder withOverflow(MetadataLevel overflowLevel) {
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

    public List<FileMetadata> getFiles() {
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