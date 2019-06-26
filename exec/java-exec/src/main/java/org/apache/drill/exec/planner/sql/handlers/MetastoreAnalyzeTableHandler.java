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
package org.apache.drill.exec.planner.sql.handlers;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.metadata.MetadataAggBatch;
import org.apache.drill.exec.planner.DFSDirPartitionLocation;
import org.apache.drill.exec.planner.DFSFilePartitionLocation;
import org.apache.drill.exec.planner.FileSystemPartitionDescriptor;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.MetadataAggRel;
import org.apache.drill.exec.planner.logical.MetadataControllerRel;
import org.apache.drill.exec.planner.logical.MetadataHandlerRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlMetastoreAnalyzeTable;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.TableInfo;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.drill.exec.planner.logical.DrillRelFactories.LOGICAL_BUILDER;

/**
 * Constructs plan to be executed for collecting metadata and storing it to the metastore.
 */
public class MetastoreAnalyzeTableHandler extends DefaultSqlHandler {
  private static final Logger logger = LoggerFactory.getLogger(MetastoreAnalyzeTableHandler.class);

  public MetastoreAnalyzeTableHandler(SqlHandlerConfig config) {
    super(config);
  }

  public MetastoreAnalyzeTableHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    super(config, textPlan);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode)
      throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    final SqlMetastoreAnalyzeTable sqlAnalyzeTable = unwrap(sqlNode, SqlMetastoreAnalyzeTable.class);

    final String tableName = sqlAnalyzeTable.getName();
    final AbstractSchema drillSchema = SchemaUtilites.resolveToDrillSchema(
        config.getConverter().getDefaultSchema(), sqlAnalyzeTable.getSchemaPath());
    DrillTable table = (DrillTable) SqlHandlerUtil.getTableFromSchema(drillSchema, tableName);

    if (table == null) {
      throw UserException.validationError()
          .message("No table with given name [%s] exists in schema [%s]", tableName,
              drillSchema.getFullSchemaName())
          .build(logger);
    }

    TableType tableType = getTableType(table.getGroupScan());
    AnalyzeInfoProvider analyzeInfoProvider = AnalyzeInfoProvider.getAnalyzeInfoProvider(tableType);

    SqlIdentifier tableIdentifier = sqlAnalyzeTable.getTableIdentifier();
    SqlSelect scanSql = new SqlSelect(
        SqlParserPos.ZERO,              /* position */
        SqlNodeList.EMPTY,              /* keyword list */
        getColumnList(sqlAnalyzeTable, analyzeInfoProvider), /* select list */
        tableIdentifier,                /* from */
        null,                           /* where */
        null,                           /* group by */
        null,                           /* having */
        null,                           /* windowDecls */
        null,                           /* orderBy */
        null,                           /* offset */
        null                            /* fetch */
    );

    final ConvertedRelNode convertedRelNode = validateAndConvert(rewrite(scanSql));
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();

    RelNode relScan = convertedRelNode.getConvertedNode();

//    if(! (table instanceof DrillTable)) {
//      return DrillStatsTable.notSupported(context, tableName);
//    }
//
//    if (table instanceof DrillTable) {
//      DrillTable drillTable = (DrillTable) table;
//      final Object selection = drillTable.getSelection();
//      if (!(selection instanceof FormatSelection)) {
//        return DrillStatsTable.notSupported(context, tableName);
//      }
//      // Do not support non-parquet tables
//      FormatSelection formatSelection = (FormatSelection) selection;
//      FormatPluginConfig formatConfig = formatSelection.getFormat();
//      if (!((formatConfig instanceof ParquetFormatConfig)
//            || ((formatConfig instanceof NamedFormatPluginConfig)
//                 && ((NamedFormatPluginConfig) formatConfig).name.equals("parquet")))) {
//        return DrillStatsTable.notSupported(context, tableName);
//      }
//
//      FileSystemPlugin plugin = (FileSystemPlugin) drillTable.getPlugin();
//      DrillFileSystem fs = new DrillFileSystem(plugin.getFormatPlugin(
//          formatSelection.getFormat()).getFsConf());
//
//      Path selectionRoot = formatSelection.getSelection().getSelectionRoot();
//      if (!selectionRoot.toUri().getPath().endsWith(tableName) || !fs.getFileStatus(selectionRoot).isDirectory()) {
//        return DrillStatsTable.notSupported(context, tableName);
//      }
//      // Do not recompute statistics, if stale
//      Path statsFilePath = new Path(selectionRoot, DotDrillType.STATS.getEnding());
//      if (fs.exists(statsFilePath) && !isStatsStale(fs, statsFilePath)) {
//       return DrillStatsTable.notRequired(context, tableName);
//      }
//    }
    // Convert the query to Drill Logical plan and insert a writer operator on top.

    DrillRel drel = convertToDrel(relScan, drillSchema, table, sqlAnalyzeTable);

    Prel prel = convertToPrel(drel, validatedRowType);
    logAndSetTextPlan("Drill Physical", prel, logger);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan, logger);
    return plan;
  }

  /* Determines if the table was modified after computing statistics based on
   * directory/file modification timestamps
   */
  private boolean isStatsStale(DrillFileSystem fs, Path statsFilePath)
      throws IOException {
    long statsFileModifyTime = fs.getFileStatus(statsFilePath).getModificationTime();
    Path parentPath = statsFilePath.getParent();
    FileStatus directoryStatus = fs.getFileStatus(parentPath);
    // Parent directory modified after stats collection?
    return directoryStatus.getModificationTime() > statsFileModifyTime ||
        tableModified(fs, parentPath, statsFileModifyTime);
  }

  /* Determines if the table was modified after computing statistics based on
   * directory/file modification timestamps. Recursively checks sub-directories.
   */
  private boolean tableModified(DrillFileSystem fs, Path parentPath,
      long statsModificationTime) throws IOException {
    for (final FileStatus file : fs.listStatus(parentPath)) {
      // If directory or files within it are modified
      if (file.getModificationTime() > statsModificationTime) {
        return true;
      }
      // For a directory, we should recursively check sub-directories
      if (file.isDirectory() && tableModified(fs, file.getPath(), statsModificationTime)) {
        return true;
      }
    }
    return false;
  }

  /* Generates the column list specified in the ANALYZE statement */
  private SqlNodeList getColumnList(SqlMetastoreAnalyzeTable sqlAnalyzeTable, AnalyzeInfoProvider analyzeInfoProvider) {
    SqlNodeList columnList = sqlAnalyzeTable.getFieldList();
    // TODO: issue when columns list specified without partition columns
    if (columnList == null || columnList.size() <= 0) {
      columnList = new SqlNodeList(SqlParserPos.ZERO);
      columnList.add(new SqlIdentifier(SchemaPath.STAR_COLUMN.rootName(), SqlParserPos.ZERO));
    }
    MetadataType metadataLevel = getMetadataType(sqlAnalyzeTable);
    for (SqlIdentifier field : analyzeInfoProvider.getProjectionFields(metadataLevel, context.getPlannerSettings().getOptions())) {
      columnList.add(field);
    }
    return columnList;
  }

  private MetadataType getMetadataType(SqlMetastoreAnalyzeTable sqlAnalyzeTable) {
    SqlCharStringLiteral level = (SqlCharStringLiteral) sqlAnalyzeTable.getLevel();
    return level != null ? MetadataType.valueOf(level.toValue().toUpperCase()) : MetadataType.ALL;
  }

  /* Converts to Drill logical plan */
  protected DrillRel convertToDrel(RelNode relNode, AbstractSchema schema, DrillTable table, SqlMetastoreAnalyzeTable sqlAnalyzeTable) throws SqlUnsupportedException, IOException {
    // TODO: add logic to create required partition descriptor depending on the table type, i.e. file system, hive, parquet etc.
    MetadataType metadataLevel = getMetadataType(sqlAnalyzeTable);

    TableType tableType = getTableType(table.getGroupScan());

    AnalyzeInfoProvider analyzeInfoProvider = AnalyzeInfoProvider.getAnalyzeInfoProvider(tableType);

    List<NamedExpression> segmentExpressions = analyzeInfoProvider.getSegmentColumns((TableScan) convertToRawDrel(relNode), context.getPlannerSettings()).stream()
        .map(partitionName ->
            new NamedExpression(partitionName, FieldReference.getWithQuotedRef(partitionName.getRootSegmentPath())))
        .collect(Collectors.toList());

    // add partition columns to the projection for the case when
    // columns list was provided in analyze statement
    if (sqlAnalyzeTable.getFieldList() != null && sqlAnalyzeTable.getFieldList().size() > 0) {
      RelBuilder relBuilder = LOGICAL_BUILDER.create(relNode.getCluster(), null);
      RelNode input = relNode.getInput(0);
      Preconditions.checkState(input.getRowType().getFieldList().get(0).isDynamicStar(), "First field should be dynamic star");
      relBuilder.push(input);

      List<RexNode> projections = segmentExpressions.stream()
          .map(namedExpression -> relBuilder.call(SqlStdOperatorTable.ITEM,
              relBuilder.field(0), relBuilder.literal(namedExpression.getRef().getRootSegmentPath())))
          .collect(Collectors.toList());

      relNode.getRowType().getFieldList().forEach(relDataTypeField -> projections.add(relBuilder.field(relDataTypeField.getName())));

      relNode = relBuilder.project(projections).build();
    }

    DrillRel convertedRelNode = convertToRawDrel(relNode);

    // TODO: investigate why it is needed?
    if (convertedRelNode instanceof DrillStoreRel) {
      throw new UnsupportedOperationException();
    }

    List<String> schemaPath = schema.getSchemaPath();
    String pluginName = schemaPath.get(0);
    String workspaceName = Strings.join(schemaPath.subList(1, schemaPath.size()), AbstractSchema.SCHEMA_SEPARATOR);

    TableInfo tableInfo = TableInfo.builder()
        .name(sqlAnalyzeTable.getName())
        .owner(table.getUserName())
        .type(tableType.name())
        .storagePlugin(pluginName)
        .workspace(workspaceName)
        .build();

    boolean createNewAggregations = true;

    String rowGroupIndexColumn = config.getContext().getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL);
    String lastModifiedTimeColumn = config.getContext().getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL);

    // columns which are not added to the schema and for which statistics is not calculated
    List<SchemaPath> excludedColumns;

    SchemaPath locationField = SchemaPath.getSimplePath(config.getContext().getOptions().getString(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL));
    SchemaPath lastModifiedTimeField = SchemaPath.getSimplePath(lastModifiedTimeColumn);
    if (tableType == TableType.PARQUET && metadataLevel.compareTo(MetadataType.ROW_GROUP) >= 0) {

      SchemaPath rgiField = SchemaPath.getSimplePath(rowGroupIndexColumn);

      excludedColumns = Arrays.asList(lastModifiedTimeField, locationField, rgiField);

      // adds aggregation for collecting row group level metadata
      List<NamedExpression> rowGroupGroupByExpressions = Arrays.asList(
          new NamedExpression(rgiField,
              FieldReference.getWithQuotedRef(rowGroupIndexColumn)),
          new NamedExpression(locationField,
              FieldReference.getWithQuotedRef(MetadataAggBatch.LOCATION_FIELD)));

      convertedRelNode = new MetadataAggRel(convertedRelNode.getCluster(),
          convertedRelNode.getTraitSet(),
          convertedRelNode,
          rowGroupGroupByExpressions,
          null,
          createNewAggregations,
          excludedColumns);

      convertedRelNode =
          new MetadataHandlerRel(convertedRelNode.getCluster(),
              convertedRelNode.getTraitSet(),
              convertedRelNode,
              tableInfo,
              Collections.singletonList(new MetadataInfo(MetadataType.ROW_GROUP, MetadataInfo.GENERAL_INFO_KEY, null)),
              MetadataType.ROW_GROUP);

      createNewAggregations = false;
      locationField = SchemaPath.getSimplePath(MetadataAggBatch.LOCATION_FIELD);
    }

    if (metadataLevel.compareTo(MetadataType.FILE) >= 0) {

      excludedColumns = Arrays.asList(lastModifiedTimeField, locationField);

      NamedExpression locationExpression = new NamedExpression(locationField, FieldReference.getWithQuotedRef(MetadataAggBatch.LOCATION_FIELD));
      ArrayList<NamedExpression> fileGroupByExpressions = new ArrayList<>(segmentExpressions);
      fileGroupByExpressions.add(locationExpression);

      convertedRelNode = new MetadataAggRel(convertedRelNode.getCluster(),
          convertedRelNode.getTraitSet(),
          convertedRelNode,
          fileGroupByExpressions,
          null,
          createNewAggregations, excludedColumns);

      convertedRelNode =
          new MetadataHandlerRel(convertedRelNode.getCluster(),
              convertedRelNode.getTraitSet(),
              convertedRelNode,
              tableInfo,
              Collections.singletonList(new MetadataInfo(MetadataType.FILE, MetadataInfo.GENERAL_INFO_KEY, null)),
              MetadataType.FILE);

      locationField = SchemaPath.getSimplePath(MetadataAggBatch.LOCATION_FIELD);

      createNewAggregations = false;
    }

    if (metadataLevel.compareTo(MetadataType.SEGMENT) >= 0) {

      for (int i = segmentExpressions.size() + 1; i > 1; i--) {
        // value for location field may be changed, so list is recreated
        excludedColumns = Arrays.asList(lastModifiedTimeField, locationField);

        List<NamedExpression> groupByExpressions = new ArrayList<>();
        groupByExpressions.add(new NamedExpression(new FunctionCall("parentPath",
            Collections.singletonList(locationField), ExpressionPosition.UNKNOWN),
            FieldReference.getWithQuotedRef(MetadataAggBatch.LOCATION_FIELD)));

        groupByExpressions.addAll(segmentExpressions);

        convertedRelNode = new MetadataAggRel(convertedRelNode.getCluster(),
            convertedRelNode.getTraitSet(),
            convertedRelNode,
            groupByExpressions.subList(0, i),
            null,
            createNewAggregations, excludedColumns);

        convertedRelNode =
            new MetadataHandlerRel(convertedRelNode.getCluster(),
                convertedRelNode.getTraitSet(),
                convertedRelNode,
                tableInfo,
                Collections.singletonList(new MetadataInfo(MetadataType.SEGMENT, MetadataInfo.GENERAL_INFO_KEY, null)),
                MetadataType.SEGMENT);

        locationField = SchemaPath.getSimplePath(MetadataAggBatch.LOCATION_FIELD);

        createNewAggregations = false;
      }
    }

    if (metadataLevel.compareTo(MetadataType.TABLE) >= 0) {
      excludedColumns = Arrays.asList(locationField, lastModifiedTimeField);

      convertedRelNode = new MetadataAggRel(convertedRelNode.getCluster(),
          convertedRelNode.getTraitSet(),
          convertedRelNode,
          Collections.emptyList(),
          null,
          createNewAggregations, excludedColumns);

      convertedRelNode =
          new MetadataHandlerRel(convertedRelNode.getCluster(),
              convertedRelNode.getTraitSet(),
              convertedRelNode,
              tableInfo,
              Collections.singletonList(new MetadataInfo(MetadataType.TABLE, MetadataInfo.GENERAL_INFO_KEY, null)),
              MetadataType.TABLE);

      convertedRelNode = new MetadataControllerRel(convertedRelNode.getCluster(),
          convertedRelNode.getTraitSet(),
          convertedRelNode, tableInfo, ((FormatSelection) table.getSelection()).getSelection().getSelectionRoot(), null);
    } else {
      throw new IllegalStateException("Analyze table with NONE level");
    }

    return new DrillScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }

  private TableType getTableType(GroupScan groupScan) {
    if (groupScan instanceof ParquetGroupScan) {
      return TableType.PARQUET;
    }
    throw new UnsupportedOperationException("Unsupported table type");
  }

  public enum TableType {
    PARQUET
  }

  public interface AnalyzeInfoProvider {
    List<SchemaPath> getSegmentColumns(TableScan tableScan, PlannerSettings plannerSettings);
    List<SqlIdentifier> getProjectionFields(MetadataType metadataLevel, OptionManager options);

    static AnalyzeInfoProvider getAnalyzeInfoProvider(TableType tableType) {
      switch (tableType) {
        case PARQUET:
          return AnalyzeFileInfoProvider.INSTANCE;
        default:
          throw new UnsupportedOperationException("Unsupported table type");
      }
    }
  }

  private static class AnalyzeFileInfoProvider implements AnalyzeInfoProvider {
    public static final AnalyzeInfoProvider INSTANCE = new AnalyzeFileInfoProvider();

    @Override
    public List<SchemaPath> getSegmentColumns(TableScan tableScan, PlannerSettings plannerSettings) {
      String partitionLabel = plannerSettings.getOptions().getString(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);

      return IntStream.range(0, getSegmentsCount(tableScan, plannerSettings))
          .mapToObj(value -> SchemaPath.getSimplePath(partitionLabel + value))
          .collect(Collectors.toList());
    }

    // TODO: cleanup this method
    private int getSegmentsCount(TableScan tableScan, PlannerSettings plannerSettings) {
      FileSystemPartitionDescriptor partitionDescriptor =
          new FileSystemPartitionDescriptor(plannerSettings, tableScan);

      return Lists.newArrayList(partitionDescriptor.iterator()).stream()
          .flatMap(Collection::stream)
          .mapToInt(
              e -> {
                if (e instanceof DFSFilePartitionLocation) {
                  return 0;
                } else if (e instanceof DFSDirPartitionLocation) {
                  return (int) Arrays.stream(((DFSDirPartitionLocation) e).getDirs())
                      .filter(Objects::nonNull)
                      .count();
                }
                return 0;
              })
          .max()
          .orElse(0);
    }

    @Override
    public List<SqlIdentifier> getProjectionFields(MetadataType metadataLevel, OptionManager options) {
      List<SqlIdentifier> columnList = new ArrayList<>();
      columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL), SqlParserPos.ZERO));
      if (metadataLevel.compareTo(MetadataType.ROW_GROUP) >= 0) {
        columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL), SqlParserPos.ZERO));
      }
      columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL), SqlParserPos.ZERO));
      return columnList;
    }
  }
}
