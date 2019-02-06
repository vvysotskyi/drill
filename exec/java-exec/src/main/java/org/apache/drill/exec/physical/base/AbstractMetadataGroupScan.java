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
import org.apache.drill.exec.expr.stat.ParquetFilterPredicate;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.parquet.ParquetRGFilterEvaluator;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
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
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractMetadataGroupScan extends AbstractFileGroupScan {
  // AbstractFileGroupScanWithMetadata ?
  // A lot of abstract classes
  // TODO: rewrite initialization of metadata using metadata provider.
  protected TableMetadataProvider metadataProvider;

  // table metadata info
  protected TableMetadata tableMetadata; // TODO: may be null, but there no everywhere null checks. Add it
  protected String tableLocation;
  protected String tableName;

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

  protected AbstractMetadataGroupScan(String userName, List<SchemaPath> columns, LogicalExpression filter) {
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
    // TODO: may return wrong results for the case when the same table metadata is used after partitions/files/row groups pruning
    long tableRowCount = (long) TableStatistics.ROW_COUNT.getValue(tableMetadata);
    long colNulls = (long) tableMetadata.getColumnStats(column).getStatistic(ColumnStatisticsKind.NULLS_COUNT);
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
    long rowCount = 0;
    for (FileMetadata file : files) {
      rowCount += (long) file.getStatistic(TableStatistics.ROW_COUNT);
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
  public AbstractMetadataGroupScan applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
                                               FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {

    // Builds filter for pruning. If filter cannot be built, null should be returned.
    FilterPredicate filterPredicate = getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, optionManager, true);
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
        && (builder.tableMetadata.isEmpty() || ((builder.getPartitions() == null
            || builder.getPartitions().isEmpty()) && partitions.size() > 0) || builder.files == null)) {
      if (files.size() == 1) {
        // For the case when group scan has single row group and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      logger.debug("All files have been filtered out. Add back one to get schema from scanner");
      builder.withMatching(false);
      PartitionMetadata nextPart = partitions.iterator().next();
      FileMetadata nextFile = files.iterator().next();
      builder.withPartitions(Collections.singletonList(nextPart));
      builder.withFiles(Collections.singletonList(nextFile));
    }

    logger.debug("applyFilter() {} reduce file # from {} to {}",
        ExpressionStringBuilder.toString(filterExpr), files.size(), builder.files.size());

    return builder.build();
  }

  protected abstract GroupScanBuilder getBuilder();

  protected List<FileMetadata> pruneFilesForPartitions(List<PartitionMetadata> filteredPartitionMetadata) {
    List<FileMetadata> prunedFiles = new ArrayList<>();
    for (FileMetadata file : files) {
      for (PartitionMetadata filteredPartition : filteredPartitionMetadata) {
        if (filteredPartition.getLocation().contains(file.getLocation())) {
          prunedFiles.add(file);
          break;
        }
      }
    }

    return prunedFiles;
  }

  protected void filterFileMetadata(OptionManager optionManager, FilterPredicate filterPredicate,
                                    Set<SchemaPath> schemaPathsInExpr, GroupScanBuilder builder) {
    if ((builder.partitions != null && builder.partitions.size() > 0) || partitions.size() == 0) {
      List<FileMetadata> prunedFiles;
      if (partitions.size() == 0 || partitions.size() == builder.partitions.size()) {
        // no partition pruning happened, no need to prune initial files list
        prunedFiles = files;
      } else {
        // prunes files to leave only files which are contained by pruned partitions
        prunedFiles = pruneFilesForPartitions(builder.partitions);
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
        List<FileMetadata> filteredFiles = filterAndGetMetadata(schemaPathsInExpr, prunedFiles, filterPredicate);

        builder.withFiles(filteredFiles)
            .withMatching(matchAllRowGroups);

        matchAllRowGroups = matchAllRowGroupsLocal;
      } else {
        builder.withMatching(false)
          .withFiles(prunedFiles)
          .withOverflow(MetadataLevel.FILE);
      }

    }
  }

  protected void filterPartitionMetadata(OptionManager optionManager, FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr, GroupScanBuilder builder) {
    if (builder.tableMetadata.size() > 0 && !builder.matchAllRowGroups) {
      if (partitions.size() > 0) {
        if (partitions.size() <= optionManager.getOption(
          PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
          boolean matchAllRowGroupsLocal = matchAllRowGroups;
          matchAllRowGroups = true;
          List<PartitionMetadata> filteredPartitionMetadata = filterAndGetMetadata(schemaPathsInExpr, partitions, filterPredicate);
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

  protected void filterTableMetadata(FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr, GroupScanBuilder builder) {
    boolean matchAllRowGroupsLocal = matchAllRowGroups;
    // Filters table metadata. If resulting list is empty, should be used single minimum entity of metadata.
    // If table matches fully, nothing is pruned and pruning of underlying metadata is stopped.
    matchAllRowGroups = true;
    List<TableMetadata> filteredTableMetadata =
        filterAndGetMetadata(schemaPathsInExpr, Collections.singletonList(this.tableMetadata), filterPredicate);

    builder.withTable(filteredTableMetadata);
    builder.withMatching(matchAllRowGroups);
    matchAllRowGroups = matchAllRowGroupsLocal;
  }

  protected <T extends BaseMetadata> List<T> filterAndGetMetadata(Set<SchemaPath> schemaPathsInExpr,
                                                                  Iterable<T> metadataList,
                                                                  FilterPredicate filterPredicate) {
    List<T> qualifiedFiles = new ArrayList<>();

    for (T metadata : metadataList) {
      // TODO: decide where implicit + partition columns should be handled: either they should be present in
      //  file metadata or they should be populated in this method and passed with other columns.

      ParquetFilterPredicate.RowsMatch match = ParquetRGFilterEvaluator.matches(filterPredicate,
          metadata.getColumnStatistics(), (long) metadata.getStatistic(TableStatistics.ROW_COUNT),
          metadata.getSchema(), schemaPathsInExpr);
      if (match == ParquetFilterPredicate.RowsMatch.NONE) {
        continue; // No file comply to the filter => drop the file
      }
      if (matchAllRowGroups) {
        matchAllRowGroups = match == ParquetFilterPredicate.RowsMatch.ALL;
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

    TupleMetadata types = tableMetadata.getSchema().copy();

    Set<SchemaPath> schemaPathsInExpr = filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);

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

  // limit push down methods start
  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    GroupScanBuilder builder = limitFiles(maxRecords);
    if (builder.getTableMetadata() == null) {
      logger.debug("limit push down does not apply, since table has less rows.");
      return null;
    }
    if ((builder.getPartitions() == null || partitions.size() == builder.getPartitions().size()) && !partitions.isEmpty()) {
      logger.debug("limit push down does not apply, since number of partitions was not reduced.");
      return null;
    }
    if (builder.getFiles() == null || files.size() == builder.getFiles().size()) {
      logger.debug("limit push down does not apply, since number of files was not reduced.");
      return null;
    }
    logger.debug("applyLimit() reduce files # from {} to {}.", files.size(), builder.getFiles().size());
    return builder.build();
  }

  protected GroupScanBuilder limitFiles(int maxRecords) {
    GroupScanBuilder builder = getBuilder();
    // further optimization : minimize # of files chosen, or the affinity of files chosen.

    long tableRowCount = (long) tableMetadata.getStatistic(TableStatistics.ROW_COUNT);
    if (tableRowCount <= maxRecords) {
      logger.debug("limit push down does not apply, since total number of rows [{}] is less or equal to the required [{}].",
          tableRowCount, maxRecords);
      return builder;
    }

    List<PartitionMetadata> qualifiedPartitions = limitMetadata(partitions, maxRecords);

    List<FileMetadata> partFiles = partitions.size() > 0 ? pruneFilesForPartitions(partitions) : files;

    // Calculate number of files to read based on maxRecords and update
    // number of records to read for each of those rowGroups.
    List<FileMetadata> qualifiedFiles = limitMetadata(partFiles, maxRecords);

    return builder
      .withTable(Collections.singletonList(tableMetadata))
      .withPartitions(qualifiedPartitions)
      .withFiles(qualifiedFiles);
  }

  protected <T extends BaseMetadata> List<T> limitMetadata(List<T> metadataList, int maxRecords) {
    List<T> qualifiedMetadata = new ArrayList<>();
    int currentRowCount = 0;
    for (T metadata : metadataList) {
      long rowCount = (long) metadata.getStatistic(ColumnStatisticsKind.ROW_COUNT);
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
    return partitions.stream()
        .filter(partition -> partition.getColumn().equals(column) && partition.getLocation().contains(path))
        .findAny()
        .map(metadata -> clazz.cast(metadata.getColumnStatistics().get(column).getStatistic(ColumnStatisticsKind.MAX_VALUE)))
        .orElse(null);
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

    if (fileSet == null) {
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

  private boolean isImplicitOrPartCol(SchemaPath schemaPath, OptionManager optionManager) {
    Set<String> implicitColNames = ColumnExplorer.initImplicitFileColumns(optionManager).keySet();
    return ColumnExplorer.isPartitionColumn(optionManager, schemaPath) || implicitColNames.contains(schemaPath.getRootSegmentPath());
  }

  protected abstract static class GroupScanBuilder {
    protected boolean matchAllRowGroups = false;

    protected List<TableMetadata> tableMetadata;
    protected List<PartitionMetadata> partitions;
    protected List<FileMetadata> files;
    protected MetadataLevel overflowLevel = MetadataLevel.NONE;

    public abstract AbstractMetadataGroupScan build();

    public GroupScanBuilder withTable(List<TableMetadata> filteredTableMetadata) {
      this.tableMetadata = filteredTableMetadata;
      return this;
    }

    public GroupScanBuilder withPartitions(List<PartitionMetadata> partitions) {
      this.partitions = partitions;
      return this;
    }

    public GroupScanBuilder withFiles(List<FileMetadata> files) {
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
