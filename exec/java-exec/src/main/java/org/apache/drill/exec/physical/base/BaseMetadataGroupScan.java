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

public abstract class BaseMetadataGroupScan extends AbstractFileGroupScan {
  protected TableMetadata tableMetadata;
  protected List<SchemaPath> columns;
  protected List<SchemaPath> partitionColumns;
  protected LogicalExpression filter;

  protected List<FileMetadata> files;

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
  public GroupScan applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
                               FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {

    if (files.size() == 1 ||
      // TODO: rename the option, value is the same, but is applied not only to row groups pruning
      files.size() > optionManager.getOption(PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
      // Stop pruning for 2 cases:
      //    -  1 single parquet file,
      //    -  # of files is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      return null;
    }

//    List<FileMetadata> fileMetadataList = tableMetadata.getFiles();

    List<FileMetadata> qualifiedFiles = filterFiles(filterExpr, udfUtilities, functionImplementationRegistry);
    if (qualifiedFiles == null) {
      return null;
    }
    if (qualifiedFiles.size() == files.size()) {
      // There is no reduction of rowGroups. Return the original groupScan.
      logger.debug("applyFilter does not have any pruning!");
      return null;
    } else if (qualifiedFiles.size() == 0) {
      logger.debug("All rows have been filtered out. Add back one to get schema from scanner");
      qualifiedFiles.add(files.iterator().next());
    }

    logger.debug("applyFilter {} reduce file # from {} to {}",
      ExpressionStringBuilder.toString(filterExpr), files.size(), qualifiedFiles.size());

    try {
      return cloneWithFileSet(qualifiedFiles);

    } catch (IOException e) {
      logger.warn("Could not apply filter prune due to Exception : {}", e);
      return null;
    }
  }
  // filter push down methods block end

  protected List<FileMetadata> filterFiles(LogicalExpression filterExpr, UdfUtilities udfUtilities,
                                         FunctionImplementationRegistry functionImplementationRegistry) {
    List<FileMetadata> qualifiedFiles = new ArrayList<>();
    if (files.size() > 0) {
      FilterPredicate filterPredicate = buildFilter(filterExpr, udfUtilities, functionImplementationRegistry, tableMetadata.getFields());
      if (filterPredicate == null) {
        return null;
      }

      final Set<SchemaPath> schemaPathsInExpr =
        filterExpr.accept(new ParquetRGFilterEvaluator.FieldReferenceFinder(), null);

      List<TableMetadata> filteredTableMetadata =
        filterMetadata(schemaPathsInExpr, Collections.singletonList(this.tableMetadata), filterPredicate);

      if (filteredTableMetadata.isEmpty()) {
        logger.debug("All rows have been filtered out during applying the filter to the table itself.");
      } else {
        qualifiedFiles.addAll(filterMetadata(schemaPathsInExpr, files, filterPredicate));
      }
    }
    return qualifiedFiles;
  }

  protected <T extends BaseMetadata> List<T> filterMetadata(Set<SchemaPath> schemaPathsInExpr,
                                                          Iterable<T> fileMetadataList,
                                                          FilterPredicate filterPredicate) {
    List<T> qualifiedFiles = new ArrayList<>();

    for (T fileMetadata : fileMetadataList) {
      // TODO: decide where implicit + partition columns should be handled: either they should be present in
      // file metadata or they should be populated in this method and passed with other columns.

      ParquetFilterPredicate.RowsMatch match = ParquetRGFilterEvaluator.matches(filterPredicate,
          fileMetadata.getColumnStatistics(), (long) fileMetadata.getStatistic(TableStatistics.ROW_COUNT),
          fileMetadata.getFields(), schemaPathsInExpr);
      if (match == ParquetFilterPredicate.RowsMatch.NONE) {
        continue; // No file comply to the filter => drop the row group
      }
      if (matchAllRowGroups) {
        matchAllRowGroups = match == ParquetFilterPredicate.RowsMatch.ALL;
      }
      qualifiedFiles.add(fileMetadata);
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
    return files.stream()
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
      fileSet = files.stream()
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
  protected abstract BaseMetadataGroupScan cloneWithFileSet(Collection<FileMetadata> files) throws IOException;
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
  private int updateRowGroupInfo(int maxRecords) {
    long count = 0;
    int index = 0;

    for (FileMetadata fileMetadata : files) {
      long rowCount = (long) fileMetadata.getStatistic(() -> "rowCount");
      if (count + rowCount <= maxRecords) {
        count += rowCount;
        // TODO: allow passing number records to be read for the file
//        fileMetadata.setNumRecordsToRead(rowCount);
        index++;
        continue;
      } else if (count < maxRecords) {
//        fileMetadata.setNumRecordsToRead(maxRecords - count);
        index++;
      }
      break;
    }
    return index;
  }
}
