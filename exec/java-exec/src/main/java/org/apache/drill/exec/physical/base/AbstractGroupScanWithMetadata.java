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
import org.apache.drill.metastore.ColumnStatistics;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.LocationProvider;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.TableMetadata;
import org.apache.drill.metastore.TableStatisticsKind;
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

/**
 * Represents table group scan with metadata usage.
 */
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

  protected AbstractGroupScanWithMetadata(AbstractGroupScanWithMetadata that) {
    super(that.getUserName());
    this.columns = that.columns;
    this.filter = that.filter;
    this.matchAllRowGroups = that.matchAllRowGroups;

    this.metadataProvider = that.metadataProvider;
    this.tableMetadata = that.tableMetadata;
    this.partitionColumns = that.partitionColumns;
    this.partitions = that.partitions;
    this.files = that.files;

    this.fileSet = that.fileSet == null ? null : new HashSet<>(that.fileSet);
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
    long tableRowCount = (long) TableStatisticsKind.ROW_COUNT.getValue(getTableMetadata());
    ColumnStatistics columnStats = getTableMetadata().getColumnStatistics(column);
    long colNulls;
    if (columnStats != null) {
      Long nulls = (Long) columnStats.getStatistic(ColumnStatisticsKind.NULLS_COUNT);
      colNulls = nulls != null ? nulls : GroupScan.NO_COLUMN_STATS;
    } else {
      return 0;
    }
    return GroupScan.NO_COLUMN_STATS == tableRowCount
        || GroupScan.NO_COLUMN_STATS == colNulls
        ? GroupScan.NO_COLUMN_STATS : tableRowCount - colNulls;
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public ScanStats getScanStats() {
    int columnCount = columns == null ? 20 : columns.size();
    double rowCount = (long) TableStatisticsKind.ROW_COUNT.getValue(getTableMetadata());

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

    GroupScanWithMetadataFilterer filteredMetadata = getFilterer().getFiltered(optionManager, filterPredicate, schemaPathsInExpr);

    if (getFilesMetadata() != null) {
      if (filteredMetadata.getFiles() != null && getFilesMetadata().size() == filteredMetadata.getFiles().size()) {
        // There is no reduction of files, but filter may be omitted.
        logger.debug("applyFilter() does not have any pruning since GroupScan fully matches filter");
        matchAllRowGroups = filteredMetadata.isMatchAllRowGroups();
        return null;
      }
    } else if (getPartitionsMetadata() != null) {
      // for the case when files metadata wasn't created, check partition metadata
      if (filteredMetadata.getPartitions() != null && getPartitionsMetadata().size() == filteredMetadata.getPartitions().size()) {
        // There is no reduction of partitions, but filter may be omitted.
        logger.debug("applyFilter() does not have any pruning since GroupScan fully matches filter");
        matchAllRowGroups = filteredMetadata.isMatchAllRowGroups();
        return null;
      }
    } else if (getTableMetadata() != null) {
      // There is no reduction of files, but filter may be omitted.
      logger.debug("applyFilter() does not have any pruning since GroupScan fully matches filter");
      matchAllRowGroups = filteredMetadata.isMatchAllRowGroups();
      return null;
    }

    if (!filteredMetadata.isMatchAllRowGroups()
      // filter returns empty result using table metadata
      && (((filteredMetadata.getTableMetadata() == null || filteredMetadata.getTableMetadata().isEmpty()) && getTableMetadata() != null)
          // all partitions pruned if partition metadata is available
          || unchangedMetadata(getPartitionsMetadata(), filteredMetadata.getPartitions())
          // all files are pruned if file metadata is available
          || unchangedMetadata(getFilesMetadata(), filteredMetadata.getFiles()))) {
      if (getFilesMetadata().size() == 1) {
        // For the case when group scan has single file and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      logger.debug("All files have been filtered out. Add back one to get schema from scanner");
      filteredMetadata.withTable(getTableMetadata() != null ? Collections.singletonList(getTableMetadata()) : Collections.emptyList())
          .withPartitions(getNextOrEmpty(getPartitionsMetadata()))
          .withFiles(getNextOrEmpty(getFilesMetadata()))
          .withMatching(false);
    }

    return filteredMetadata.build();
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

  protected abstract GroupScanWithMetadataFilterer getFilterer();

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
    return getTableMetadata().getSchema().copy();
  }

  // limit push down methods start
  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    GroupScanWithMetadataFilterer prunedMetadata = limitFiles(maxRecords);
    if (getTableMetadata() != null && (prunedMetadata.getTableMetadata() == null || prunedMetadata.getTableMetadata().isEmpty())) {
      logger.debug("limit push down does not apply, since table has less rows.");
      return null;
    }
    if (getFilesMetadata() != null && !getFilesMetadata().isEmpty() && (prunedMetadata.getFiles() == null || getFilesMetadata().size() == prunedMetadata.getFiles().size() || prunedMetadata.getFiles().isEmpty())) {
      logger.debug("limit push down does not apply, since number of files was not reduced.");
      return null;
    }
    if (getPartitionsMetadata() != null && !getPartitionsMetadata().isEmpty() && (prunedMetadata.getPartitions() == null || getPartitionsMetadata().size() == prunedMetadata.getPartitions().size() || prunedMetadata.getPartitions().isEmpty())) {
      logger.debug("limit push down does not apply, since number of partitions was not reduced.");
      return null;
    }
    return prunedMetadata.build();
  }

  protected GroupScanWithMetadataFilterer limitFiles(int maxRecords) {
    GroupScanWithMetadataFilterer prunedMetadata = getFilterer();
    // further optimization : minimize # of files chosen, or the affinity of files chosen.

    if (getTableMetadata() != null) {
      long tableRowCount = (long) getTableMetadata().getStatistic(TableStatisticsKind.ROW_COUNT);
      if (tableRowCount <= maxRecords) {
        logger.debug("limit push down does not apply, since total number of rows [{}] is less or equal to the required [{}].",
            tableRowCount, maxRecords);
        return prunedMetadata;
      }
    }

    List<PartitionMetadata> qualifiedPartitions = limitMetadata(getPartitionsMetadata() != null ? getPartitionsMetadata() : Collections.emptyList(), maxRecords);

    List<FileMetadata> partFiles = getPartitionsMetadata() != null && !getPartitionsMetadata().isEmpty() ? GroupScanWithMetadataFilterer.pruneForPartitions(getFilesMetadata(), qualifiedPartitions) : getFilesMetadata();

    // Calculate number of files to read based on maxRecords and update
    // number of records to read for each of those rowGroups.
    List<FileMetadata> qualifiedFiles = limitMetadata(partFiles != null ? partFiles : Collections.emptyList(), maxRecords);

    return prunedMetadata
        .withTable(getTableMetadata() != null ? Collections.singletonList(getTableMetadata()) : Collections.emptyList())
        .withPartitions(qualifiedPartitions)
        .withFiles(qualifiedFiles);
  }

  protected <T extends BaseMetadata> List<T> limitMetadata(List<T> metadataList, int maxRecords) {
    List<T> qualifiedMetadata = new ArrayList<>();
    int currentRowCount = 0;
    for (T metadata : metadataList) {
      long rowCount = (long) TableStatisticsKind.ROW_COUNT.getValue(metadata);
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
    ColumnMetadata columnMetadata = SchemaPathUtils.getColumnMetadata(schemaPath, getTableMetadata().getSchema());
    return columnMetadata != null ? columnMetadata.majorType() : null;
  }

  @JsonIgnore
  public <T> T getPartitionValue(String path, SchemaPath column, Class<T> clazz) {
    // TODO: add path-to-file metadata map to avoid filtering
    if (getPartitionsMetadata() != null) {
      return getPartitionsMetadata().stream()
          .filter(partition -> partition.getColumn().equals(column) && partition.getLocations().contains(path))
          .findAny()
          .map(metadata -> clazz.cast(metadata.getColumnsStatistics().get(column).getStatistic(ColumnStatisticsKind.MAX_VALUE)))
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
    if (fileSet == null && getFilesMetadata() != null) {
      fileSet = getFilesMetadata().stream()
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

  // protected methods for internal usage
  protected List<FileMetadata> getFilesMetadata() {
    if (files == null) {
      files = metadataProvider.getFilesMetadata();
    }
    return files;
  }

  protected TableMetadata getTableMetadata() {
    if (tableMetadata == null) {
      tableMetadata = metadataProvider.getTableMetadata();
    }
    return tableMetadata;
  }

  protected List<PartitionMetadata> getPartitionsMetadata() {
    if (partitions == null) {
      partitions = metadataProvider.getPartitionsMetadata();
    }
    return partitions;
  }

  protected abstract static class GroupScanWithMetadataFilterer {
    protected final AbstractGroupScanWithMetadata source;

    public GroupScanWithMetadataFilterer(AbstractGroupScanWithMetadata source) {
      this.source = source;
    }

    protected boolean matchAllRowGroups = false;

    protected List<TableMetadata> tableMetadata;
    protected List<PartitionMetadata> partitions;
    protected List<FileMetadata> files;
    protected MetadataLevel overflowLevel = MetadataLevel.NONE;

    public abstract AbstractGroupScanWithMetadata build();

    public GroupScanWithMetadataFilterer withTable(List<TableMetadata> filteredTableMetadata) {
      this.tableMetadata = filteredTableMetadata;
      return this;
    }

    public GroupScanWithMetadataFilterer withPartitions(List<PartitionMetadata> partitions) {
      this.partitions = partitions;
      return this;
    }

    public GroupScanWithMetadataFilterer withFiles(List<FileMetadata> files) {
      this.files = files;
      return this;
    }

    public GroupScanWithMetadataFilterer withMatching(boolean matchAllRowGroups) {
      this.matchAllRowGroups = matchAllRowGroups;
      return this;
    }

    public GroupScanWithMetadataFilterer withOverflow(MetadataLevel overflowLevel) {
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

    /**
     * Produces filtering of metadata from current group scan and returns {@link GroupScanWithMetadataFilterer}
     * to construct resulting group scan.
     *
     * @param optionManager
     * @param filterPredicate
     * @param schemaPathsInExpr
     * @return
     */
    protected GroupScanWithMetadataFilterer getFiltered(OptionManager optionManager, FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr) {
      if (source.getTableMetadata() != null) {
        filterTableMetadata(filterPredicate, schemaPathsInExpr);
      }

      if (source.getPartitionsMetadata() != null) {
        filterPartitionMetadata(optionManager, filterPredicate, schemaPathsInExpr);
      }

      if (source.getFilesMetadata() != null) {
        filterFileMetadata(optionManager, filterPredicate, schemaPathsInExpr);
      }
      return this;
    }

    protected void filterTableMetadata(FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr) {
      // Filters table metadata. If resulting list is empty, should be used single minimum entity of metadata.
      // If table matches fully, nothing is pruned and pruning of underlying metadata is stopped.
      matchAllRowGroups = true;
      tableMetadata =
          filterAndGetMetadata(schemaPathsInExpr, Collections.singletonList(source.getTableMetadata()), filterPredicate, null);
    }

    public <T extends BaseMetadata> List<T> filterAndGetMetadata(Set<SchemaPath> schemaPathsInExpr,
                                                                 Iterable<T> metadataList,
                                                                 FilterPredicate filterPredicate,
                                                                 OptionManager optionManager) {
      List<T> qualifiedFiles = new ArrayList<>();

      for (T metadata : metadataList) {
        Map<SchemaPath, ColumnStatistics> columnsStatistics = metadata.getColumnsStatistics();

        // adds partition (dir) column statistics if it may be used during filter evaluation
        if (metadata instanceof LocationProvider && optionManager != null) {
          LocationProvider locationProvider = (LocationProvider) metadata;
          columnsStatistics = ParquetTableMetadataUtils.addImplicitColumnsStatistics(columnsStatistics,
              source.columns, source.getPartitionValues(locationProvider), optionManager,
              locationProvider.getLocation(), source.supportsFileImplicitColumns());
        }

        RowsMatch match = FilterEvaluatorUtils.matches(filterPredicate,
            columnsStatistics, (long) metadata.getStatistic(TableStatisticsKind.ROW_COUNT),
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

    protected void filterPartitionMetadata(OptionManager optionManager, FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr) {
      if (!matchAllRowGroups) {
        if (!source.getPartitionsMetadata().isEmpty()) {
          if (source.getPartitionsMetadata().size() <= optionManager.getOption(
            PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
            matchAllRowGroups = true;
            partitions = filterAndGetMetadata(schemaPathsInExpr, source.getPartitionsMetadata(), filterPredicate, optionManager);
          } else {
            matchAllRowGroups = false;
            overflowLevel = MetadataLevel.PARTITION;
          }
        }
      } else {
        partitions = source.getPartitionsMetadata();
      }
    }

    protected void filterFileMetadata(OptionManager optionManager, FilterPredicate filterPredicate,
                                      Set<SchemaPath> schemaPathsInExpr) {
      List<FileMetadata> prunedFiles;
      if (source.getPartitionsMetadata() == null || source.getPartitionsMetadata().isEmpty() || source.getPartitionsMetadata().size() == getPartitions().size()) {
        // no partition pruning happened, no need to prune initial files list
        prunedFiles = source.getFilesMetadata();
      } else {
        // prunes files to leave only files which are contained by pruned partitions
        prunedFiles = pruneForPartitions(source.getFilesMetadata(), partitions);
      }

      if (matchAllRowGroups) {
        files = prunedFiles;
        return;
      }

      // Stop files pruning for the case:
      //    -  # of files is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      if (prunedFiles.size() <= optionManager.getOption(
        PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {

        matchAllRowGroups = true;
        files = filterAndGetMetadata(schemaPathsInExpr, prunedFiles, filterPredicate, optionManager);

      } else {
        matchAllRowGroups = false;
        files = prunedFiles;
        overflowLevel = MetadataLevel.FILE;
      }
    }

    public static <T extends BaseMetadata & LocationProvider> List<T> pruneForPartitions(List<T> metadataToPrune, List<PartitionMetadata> filteredPartitionMetadata) {
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
  }

  public enum MetadataLevel {
    TABLE, PARTITION, FILE, ROW_GROUP, NONE
  }
}
