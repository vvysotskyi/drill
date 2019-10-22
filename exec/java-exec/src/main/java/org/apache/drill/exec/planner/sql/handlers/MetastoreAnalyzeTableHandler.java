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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
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
import org.apache.drill.common.util.function.CheckedSupplier;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SchemalessScan;
import org.apache.drill.exec.physical.impl.metadata.MetadataAggBatch;
import org.apache.drill.exec.planner.FileSystemPartitionDescriptor;
import org.apache.drill.exec.planner.PartitionLocation;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.MetadataAggRel;
import org.apache.drill.exec.planner.logical.MetadataControllerRel;
import org.apache.drill.exec.planner.logical.MetadataHandlerRel;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.handlers.MetastoreAnalyzeTableHandler.MetadataAggregateContext.MetadataAggregateContextBuilder;
import org.apache.drill.exec.planner.sql.handlers.MetastoreAnalyzeTableHandler.MetadataControllerContext.MetadataControllerContextBuilder;
import org.apache.drill.exec.planner.sql.parser.SqlMetastoreAnalyzeTable;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.metastore.components.tables.BasicTablesRequests;
import org.apache.drill.metastore.components.tables.MetastoreTableInfo;
import org.apache.drill.metastore.components.tables.Tables;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.TableInfo;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Streams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.drill.exec.planner.logical.DrillRelFactories.LOGICAL_BUILDER;

/**
 * Constructs plan to be executed for collecting metadata and storing it to the metastore.
 */
public class MetastoreAnalyzeTableHandler extends DefaultSqlHandler {
  private static final Logger logger = LoggerFactory.getLogger(MetastoreAnalyzeTableHandler.class);

  public MetastoreAnalyzeTableHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    super(config, textPlan);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode)
      throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    if (!context.getOptions().getOption(ExecConstants.METASTORE_ENABLED_VALIDATOR)) {
      throw UserException.validationError()
          .message("Running ANALYZE command when metastore is disabled")
          .build(logger);
    }
    try {
      // disables during analyze to prevent using locations from the metastore
      context.getOptions().setLocalOption(ExecConstants.METASTORE_ENABLED, false);
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

      AnalyzeInfoProvider analyzeInfoProvider = getAnalyzeInfoProvider(getTableType(table.getGroupScan()));

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

      DrillRel drel = convertToDrel(relScan, drillSchema, table, sqlAnalyzeTable);

      Prel prel = convertToPrel(drel, validatedRowType);
      logAndSetTextPlan("Drill Physical", prel, logger);
      PhysicalOperator pop = convertToPop(prel);
      PhysicalPlan plan = convertToPlan(pop);
      log("Drill Plan", plan, logger);
      return plan;
    } finally {
      context.getOptions().setLocalOption(ExecConstants.METASTORE_ENABLED, true);
    }
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
    SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);
    columnList.add(new SqlIdentifier(SchemaPath.STAR_COLUMN.rootName(), SqlParserPos.ZERO));
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
  private DrillRel convertToDrel(RelNode relNode, AbstractSchema schema,
      DrillTable table, SqlMetastoreAnalyzeTable sqlAnalyzeTable) throws SqlUnsupportedException, IOException {
    RelBuilder relBuilder = LOGICAL_BUILDER.create(relNode.getCluster(), null);


    TableType tableType = getTableType(table.getGroupScan());

    AnalyzeInfoProvider analyzeInfoProvider = getAnalyzeInfoProvider(tableType);

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

    List<String> segmentColumns = analyzeInfoProvider.getSegmentColumns(table, context.getPlannerSettings().getOptions()).stream()
        .map(SchemaPath::getRootSegmentPath)
        .collect(Collectors.toList());
    List<NamedExpression> segmentExpressions = segmentColumns.stream()
        .map(partitionName ->
            new NamedExpression(SchemaPath.getSimplePath(partitionName), FieldReference.getWithQuotedRef(partitionName)))
        .collect(Collectors.toList());

    List<MetadataInfo> rowGroupsInfo = Collections.emptyList();
    List<MetadataInfo> filesInfo = Collections.emptyList();
    Multimap<Integer, MetadataInfo> segments = ArrayListMultimap.create();

    Tables tables = context.getMetastoreRegistry().get().tables();

    SqlNodeList fieldList = sqlAnalyzeTable.getFieldList();
    List<SchemaPath> interestingColumns = fieldList == null
        ? null
        : StreamSupport.stream(fieldList.spliterator(), false)
            .map(sqlNode -> SchemaPath.parseFromString(sqlNode.toString()))
            .collect(Collectors.toList());

    MetastoreTableInfo metastoreTableInfo = tables.basicRequests().metastoreTableInfo(tableInfo);

    List<MetadataInfo> allMetaToHandle = new ArrayList<>();
    List<MetadataInfo> metadataToRemove = new ArrayList<>();

    if (metastoreTableInfo.isExists()) {
      RelNode finalRelNode = relNode;
      CheckedSupplier<TableScan, SqlUnsupportedException> tableScanSupplier = () -> AnalyzeTableHandler.findScan(convertToDrel(finalRelNode.getInput(0)));

      MetadataInfoCollector metadataInfoCollector = analyzeInfoProvider.getMetadataInfoCollector(tables.basicRequests(), tableInfo,
          (FormatSelection) table.getSelection(), context.getPlannerSettings(), tableScanSupplier, interestingColumns, segmentColumns.size());

      if (!metadataInfoCollector.isChanged()) {
        DrillRel convertedRelNode = convertToRawDrel(
            relBuilder.values(new String[]{"ok", "Summary"}, false, "Analyze is so cool, it knows that table wasn't changed!").build());
        return new DrillScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
      }

      // updates scan to read updated / new files, pass removed files into metadata handler
      relNode = relNode.copy(relNode.getTraitSet(), Collections.singletonList(metadataInfoCollector.getPrunedScan()));

      filesInfo = metadataInfoCollector.getFilesInfo();
      segments = metadataInfoCollector.getSegmentsInfo();
      rowGroupsInfo = metadataInfoCollector.getRowGroupsInfo();

      allMetaToHandle = metadataInfoCollector.getAllMetaToHandle();
      metadataToRemove = metadataInfoCollector.getMetadataToRemove();
    }

    DrillRel convertedRelNode = convertToRawDrel(relNode);

    boolean createNewAggregations = true;

    // columns which are not added to the schema and for which statistics is not calculated
    List<SchemaPath> excludedColumns;

    // List of columns for which statistics should be collected: interesting columns + segment columns
    List<SchemaPath> statisticsColumns = interestingColumns == null
        ? null
        : new ArrayList<>(interestingColumns);
    if (statisticsColumns != null) {
      //
      statisticsColumns.addAll(
          segmentColumns.stream()
              .map(SchemaPath::getSimplePath)
              .collect(Collectors.toList()));
    }

    SchemaPath locationField = SchemaPath.getSimplePath(config.getContext().getOptions().getString(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL));
    SchemaPath lastModifiedTimeField = SchemaPath.getSimplePath(config.getContext().getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL));

    MetadataType metadataLevel = getMetadataType(sqlAnalyzeTable);

    if (tableType == TableType.PARQUET && metadataLevel.compareTo(MetadataType.ROW_GROUP) >= 0) {
      String rowGroupIndexColumn = config.getContext().getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL);
      SchemaPath rowGroupStartField = SchemaPath.getSimplePath(config.getContext().getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL));
      SchemaPath rowGroupLengthField = SchemaPath.getSimplePath(config.getContext().getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_LEHGTH_COLUMN_LABEL));
      SchemaPath rgiField = SchemaPath.getSimplePath(rowGroupIndexColumn);

      excludedColumns = Arrays.asList(lastModifiedTimeField, locationField, rgiField, rowGroupStartField, rowGroupLengthField);

      // adds aggregation for collecting row group level metadata
      List<NamedExpression> rowGroupGroupByExpressions = new ArrayList<>(segmentExpressions);
      rowGroupGroupByExpressions.add(
          new NamedExpression(rgiField,
              FieldReference.getWithQuotedRef(rowGroupIndexColumn)));

      rowGroupGroupByExpressions.add(new NamedExpression(locationField, FieldReference.getWithQuotedRef(MetadataAggBatch.LOCATION_FIELD)));

      MetadataAggregateContext aggregateContext = MetadataAggregateContext.builder()
          .groupByExpressions(rowGroupGroupByExpressions)
          .interestingColumns(statisticsColumns)
          .createNewAggregations(createNewAggregations)
          .excludedColumns(excludedColumns)
          .build();

      convertedRelNode = new MetadataAggRel(convertedRelNode.getCluster(),
          convertedRelNode.getTraitSet(), convertedRelNode, aggregateContext);

      MetadataHandlerContext handlerContext = MetadataHandlerContext.builder()
          .tableInfo(tableInfo)
          .metadataToHandle(rowGroupsInfo)
          .metadataType(MetadataType.ROW_GROUP)
          .depthLevel(segmentExpressions.size())
          .segmentColumns(segmentColumns)
          .build();

      convertedRelNode =
          new MetadataHandlerRel(convertedRelNode.getCluster(),
              convertedRelNode.getTraitSet(),
              convertedRelNode,
              handlerContext);

      createNewAggregations = false;
      locationField = SchemaPath.getSimplePath(MetadataAggBatch.LOCATION_FIELD);
    }

    if (metadataLevel.compareTo(MetadataType.FILE) >= 0) {

      excludedColumns = Arrays.asList(lastModifiedTimeField, locationField);

      NamedExpression locationExpression = new NamedExpression(locationField, FieldReference.getWithQuotedRef(MetadataAggBatch.LOCATION_FIELD));
      List<NamedExpression> fileGroupByExpressions = new ArrayList<>(segmentExpressions);
      fileGroupByExpressions.add(locationExpression);

      MetadataAggregateContext aggregateContext = MetadataAggregateContext.builder()
          .groupByExpressions(fileGroupByExpressions)
          .interestingColumns(statisticsColumns)
          .createNewAggregations(createNewAggregations)
          .excludedColumns(excludedColumns)
          .build();

      MetadataHandlerContext handlerContext = MetadataHandlerContext.builder()
          .tableInfo(tableInfo)
          .metadataToHandle(filesInfo)
          .metadataType(MetadataType.FILE)
          .depthLevel(segmentExpressions.size())
          .segmentColumns(segmentColumns)
          .build();

      convertedRelNode = new MetadataAggRel(convertedRelNode.getCluster(),
          convertedRelNode.getTraitSet(), convertedRelNode, aggregateContext);

      convertedRelNode =
          new MetadataHandlerRel(convertedRelNode.getCluster(),
              convertedRelNode.getTraitSet(),
              convertedRelNode,
              handlerContext);

      locationField = SchemaPath.getSimplePath(MetadataAggBatch.LOCATION_FIELD);

      createNewAggregations = false;
    }

    if (metadataLevel.compareTo(MetadataType.SEGMENT) >= 0) {

      for (int i = segmentExpressions.size(); i > 0; i--) {
        // value for location field may be changed, so list is recreated
        excludedColumns = Arrays.asList(lastModifiedTimeField, locationField);

        List<NamedExpression> groupByExpressions = new ArrayList<>();
        groupByExpressions.add(new NamedExpression(new FunctionCall("parentPath",
            Collections.singletonList(locationField), ExpressionPosition.UNKNOWN),
            FieldReference.getWithQuotedRef(MetadataAggBatch.LOCATION_FIELD)));

        groupByExpressions.addAll(segmentExpressions);

        MetadataAggregateContext aggregateContext = MetadataAggregateContext.builder()
            .groupByExpressions(groupByExpressions.subList(0, i + 1))
            .interestingColumns(statisticsColumns)
            .createNewAggregations(createNewAggregations)
            .excludedColumns(excludedColumns)
            .build();

        convertedRelNode = new MetadataAggRel(convertedRelNode.getCluster(),
            convertedRelNode.getTraitSet(), convertedRelNode, aggregateContext);

        MetadataHandlerContext handlerContext = MetadataHandlerContext.builder()
            .tableInfo(tableInfo)
            .metadataToHandle(new ArrayList<>(segments.get(i - 1)))
            .metadataType(MetadataType.SEGMENT)
            .depthLevel(i)
            .segmentColumns(segmentColumns.subList(0, i))
            .build();

        convertedRelNode =
            new MetadataHandlerRel(convertedRelNode.getCluster(),
                convertedRelNode.getTraitSet(),
                convertedRelNode,
                handlerContext);

        locationField = SchemaPath.getSimplePath(MetadataAggBatch.LOCATION_FIELD);

        createNewAggregations = false;
      }
    }

    if (metadataLevel.compareTo(MetadataType.TABLE) >= 0) {
      excludedColumns = Arrays.asList(locationField, lastModifiedTimeField);

      MetadataAggregateContext aggregateContext = MetadataAggregateContext.builder()
          .groupByExpressions(Collections.emptyList())
          .interestingColumns(statisticsColumns)
          .createNewAggregations(createNewAggregations)
          .excludedColumns(excludedColumns)
          .build();

      convertedRelNode = new MetadataAggRel(convertedRelNode.getCluster(),
          convertedRelNode.getTraitSet(), convertedRelNode, aggregateContext);

      MetadataHandlerContext handlerContext = MetadataHandlerContext.builder()
          .tableInfo(tableInfo)
          .metadataToHandle(Collections.emptyList())
          .metadataType(MetadataType.TABLE)
          .depthLevel(segmentExpressions.size())
          .segmentColumns(segmentColumns)
          .build();

      convertedRelNode =
          new MetadataHandlerRel(convertedRelNode.getCluster(),
              convertedRelNode.getTraitSet(),
              convertedRelNode,
              handlerContext);

      MetadataControllerContext metadataControllerContext = MetadataControllerContext.builder()
          .tableInfo(tableInfo)
          .metastoreTableInfo(metastoreTableInfo)
          .location(((FormatSelection) table.getSelection()).getSelection().getSelectionRoot())
          .interestingColumns(interestingColumns)
          .segmentColumns(segmentColumns)
          .metadataToHandle(allMetaToHandle)
          .metadataToRemove(metadataToRemove)
          .build();

      convertedRelNode = new MetadataControllerRel(convertedRelNode.getCluster(),
          convertedRelNode.getTraitSet(),
          convertedRelNode,
          metadataControllerContext);
    } else {
      throw new IllegalStateException("Analyze table with NONE level");
    }

    return new DrillScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }

  private static FileSelection getExpandedFileSelection(FileSelection fileSelection) throws IOException {
    FileSystem rawFs = fileSelection.getSelectionRoot().getFileSystem(new Configuration());
    FileSystem fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), rawFs.getConf());
    List<FileStatus> fileStatuses = DrillFileSystemUtil.listFiles(fs, fileSelection.getSelectionRoot(), true);
    fileSelection = FileSelection.create(fileStatuses, null, fileSelection.getSelectionRoot());
    return fileSelection;
  }

  private TableType getTableType(GroupScan groupScan) {
    if (groupScan instanceof ParquetGroupScan) {
      return TableType.PARQUET;
    }
    throw new UnsupportedOperationException("Unsupported table type");
  }

  @JsonDeserialize(builder = MetadataHandlerContext.MetadataHandlerContextBuilder.class)
  public static class MetadataHandlerContext {
    private final TableInfo tableInfo;
    private final List<MetadataInfo> metadataToHandle;
    private final MetadataType metadataType;
    private final int depthLevel;
    private final List<String> segmentColumns;

    private MetadataHandlerContext(MetadataHandlerContextBuilder builder) {
      this.tableInfo = builder.tableInfo;
      this.metadataToHandle = builder.metadataToHandle;
      this.metadataType = builder.metadataType;
      this.depthLevel = builder.depthLevel;
      this.segmentColumns = builder.segmentColumns;
    }

    @JsonProperty
    public TableInfo tableInfo() {
      return tableInfo;
    }

    @JsonProperty
    public List<MetadataInfo> metadataToHandle() {
      return metadataToHandle;
    }

    @JsonProperty
    public MetadataType metadataType() {
      return metadataType;
    }

    @JsonProperty
    public int depthLevel() {
      return depthLevel;
    }

    @JsonProperty
    public List<String> segmentColumns() {
      return segmentColumns;
    }

    @Override
    public String toString() {
      return new StringJoiner(",\n", MetadataHandlerContext.class.getSimpleName() + "[", "]")
          .add("tableInfo=" + tableInfo)
          .add("metadataToHandle=" + metadataToHandle)
          .add("metadataType=" + metadataType)
          .add("depthLevel=" + depthLevel)
          .add("segmentColumns=" + segmentColumns)
          .toString();
    }

    public static MetadataHandlerContextBuilder builder() {
      return new MetadataHandlerContextBuilder();
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static class MetadataHandlerContextBuilder {
      private TableInfo tableInfo;
      private List<MetadataInfo> metadataToHandle;
      private MetadataType metadataType;
      private Integer depthLevel;
      private List<String> segmentColumns;

      public MetadataHandlerContextBuilder tableInfo(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        return this;
      }

      public MetadataHandlerContextBuilder metadataToHandle(List<MetadataInfo> metadataToHandle) {
        this.metadataToHandle = metadataToHandle;
        return this;
      }

      public MetadataHandlerContextBuilder metadataType(MetadataType metadataType) {
        this.metadataType = metadataType;
        return this;
      }

      public MetadataHandlerContextBuilder depthLevel(int depthLevel) {
        this.depthLevel = depthLevel;
        return this;
      }

      public MetadataHandlerContextBuilder segmentColumns(List<String> segmentColumns) {
        this.segmentColumns = segmentColumns;
        return this;
      }

      public MetadataHandlerContext build() {
        Objects.requireNonNull(tableInfo, "tableInfo was not set");
        Objects.requireNonNull(metadataToHandle, "metadataToHandle was not set");
        Objects.requireNonNull(metadataType, "metadataType was not set");
        Objects.requireNonNull(depthLevel, "depthLevel was not set");
        Objects.requireNonNull(segmentColumns, "segmentColumns were not set");
        return new MetadataHandlerContext(this);
      }
    }
  }

  @JsonDeserialize(builder = MetadataControllerContextBuilder.class)
  public static class MetadataControllerContext {
    private final TableInfo tableInfo;
    private final MetastoreTableInfo metastoreTableInfo;
    private final Path location;
    private final List<SchemaPath> interestingColumns;
    private final List<String> segmentColumns;
    private final List<MetadataInfo> metadataToHandle;
    private final List<MetadataInfo> metadataToRemove;

    private MetadataControllerContext(MetadataControllerContextBuilder builder) {
      this.tableInfo = builder.tableInfo;
      this.metastoreTableInfo = builder.metastoreTableInfo;
      this.location = builder.location;
      this.interestingColumns = builder.interestingColumns;
      this.segmentColumns = builder.segmentColumns;
      this.metadataToHandle = builder.metadataToHandle;
      this.metadataToRemove = builder.metadataToRemove;
    }

    @JsonProperty
    public TableInfo tableInfo() {
      return tableInfo;
    }

    @JsonProperty
    public MetastoreTableInfo metastoreTableInfo() {
      return metastoreTableInfo;
    }

    @JsonProperty
    public Path location() {
      return location;
    }

    @JsonProperty
    public List<SchemaPath> interestingColumns() {
      return interestingColumns;
    }

    @JsonProperty
    public List<String> segmentColumns() {
      return segmentColumns;
    }

    @JsonProperty
    public List<MetadataInfo> metadataToHandle() {
      return metadataToHandle;
    }

    @JsonProperty
    public List<MetadataInfo> metadataToRemove() {
      return metadataToRemove;
    }

    @Override
    public String toString() {
      return new StringJoiner(",\n", MetadataControllerContext.class.getSimpleName() + "[", "]")
          .add("tableInfo=" + tableInfo)
          .add("location=" + location)
          .add("interestingColumns=" + interestingColumns)
          .add("segmentColumns=" + segmentColumns)
          .add("metadataToHandle=" + metadataToHandle)
          .add("metadataToRemove=" + metadataToRemove)
          .toString();
    }

    public static MetadataControllerContextBuilder builder() {
      return new MetadataControllerContextBuilder();
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static class MetadataControllerContextBuilder {
      private TableInfo tableInfo;
      private MetastoreTableInfo metastoreTableInfo;
      private Path location;
      private List<SchemaPath> interestingColumns;
      private List<String> segmentColumns;
      private List<MetadataInfo> metadataToHandle;
      private List<MetadataInfo> metadataToRemove;

      public MetadataControllerContextBuilder tableInfo(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        return this;
      }

      public MetadataControllerContextBuilder metastoreTableInfo(MetastoreTableInfo metastoreTableInfo) {
        this.metastoreTableInfo = metastoreTableInfo;
        return this;
      }

      public MetadataControllerContextBuilder location(Path location) {
        this.location = location;
        return this;
      }

      public MetadataControllerContextBuilder interestingColumns(List<SchemaPath> interestingColumns) {
        this.interestingColumns = interestingColumns;
        return this;
      }

      public MetadataControllerContextBuilder segmentColumns(List<String> segmentColumns) {
        this.segmentColumns = segmentColumns;
        return this;
      }

      public MetadataControllerContextBuilder metadataToHandle(List<MetadataInfo> metadataToHandle) {
        this.metadataToHandle = metadataToHandle;
        return this;
      }

      public MetadataControllerContextBuilder metadataToRemove(List<MetadataInfo> metadataToRemove) {
        this.metadataToRemove = metadataToRemove;
        return this;
      }

      public MetadataControllerContext build() {
        Objects.requireNonNull(tableInfo, "tableInfo was not set");
        Objects.requireNonNull(location, "location was not set");
        Objects.requireNonNull(segmentColumns, "segmentColumns were not set");
        Objects.requireNonNull(metadataToHandle, "metadataToHandle was not set");
        Objects.requireNonNull(metadataToRemove, "metadataToRemove was not set");
        return new MetadataControllerContext(this);
      }
    }
  }

  @JsonDeserialize(builder = MetadataAggregateContextBuilder.class)
  public static class MetadataAggregateContext {
    private final List<NamedExpression> groupByExpressions;
    private final List<SchemaPath> interestingColumns;
    private final List<SchemaPath> excludedColumns;
    private final boolean createNewAggregations;

    public MetadataAggregateContext(MetadataAggregateContextBuilder builder) {
      this.groupByExpressions = builder.groupByExpressions;
      this.interestingColumns = builder.interestingColumns;
      this.createNewAggregations = builder.createNewAggregations;
      this.excludedColumns = builder.excludedColumns;
    }

    @JsonProperty
    public List<NamedExpression> groupByExpressions() {
      return groupByExpressions;
    }

    @JsonProperty
    public List<SchemaPath> interestingColumns() {
      return interestingColumns;
    }

    @JsonProperty
    public boolean createNewAggregations() {
      return createNewAggregations;
    }

    @JsonProperty
    public List<SchemaPath> excludedColumns() {
      return excludedColumns;
    }

    @Override
    public String toString() {
      return new StringJoiner(",\n", MetadataAggregateContext.class.getSimpleName() + "[", "]")
          .add("groupByExpressions=" + groupByExpressions)
          .add("interestingColumns=" + interestingColumns)
          .add("createNewAggregations=" + createNewAggregations)
          .add("excludedColumns=" + excludedColumns)
          .toString();
    }

    public static MetadataAggregateContextBuilder builder() {
      return new MetadataAggregateContextBuilder();
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static class MetadataAggregateContextBuilder {
      private List<NamedExpression> groupByExpressions;
      private List<SchemaPath> interestingColumns;
      private Boolean createNewAggregations;
      private List<SchemaPath> excludedColumns;

      public MetadataAggregateContextBuilder groupByExpressions(List<NamedExpression> groupByExpressions) {
        this.groupByExpressions = groupByExpressions;
        return this;
      }

      public MetadataAggregateContextBuilder interestingColumns(List<SchemaPath> interestingColumns) {
        this.interestingColumns = interestingColumns;
        return this;
      }

      public MetadataAggregateContextBuilder createNewAggregations(boolean createNewAggregations) {
        this.createNewAggregations = createNewAggregations;
        return this;
      }

      public MetadataAggregateContextBuilder excludedColumns(List<SchemaPath> excludedColumns) {
        this.excludedColumns = excludedColumns;
        return this;
      }

      public MetadataAggregateContext build() {
        Objects.requireNonNull(groupByExpressions, "groupByExpressions were not set");
        Objects.requireNonNull(createNewAggregations, "createNewAggregations was not set");
        Objects.requireNonNull(excludedColumns, "excludedColumns were not set");
        return new MetadataAggregateContext(this);
      }
    }
  }

  public enum TableType {
    PARQUET
  }

  public static AnalyzeInfoProvider getAnalyzeInfoProvider(TableType tableType) {
    switch (tableType) {
      case PARQUET:
        return AnalyzeFileInfoProvider.INSTANCE;
      default:
        throw new UnsupportedOperationException("Unsupported table type");
    }
  }

  public interface MetadataInfoCollector {
    List<MetadataInfo> getRowGroupsInfo();
    List<MetadataInfo> getFilesInfo();
    Multimap<Integer, MetadataInfo> getSegmentsInfo();
    List<MetadataInfo> getAllMetaToHandle();
    List<MetadataInfo> getMetadataToRemove();
    TableScan getPrunedScan();
    boolean isChanged();
  }

  public static class FileMetadataInfoCollector implements MetadataInfoCollector {
    private final BasicTablesRequests basicRequests;
    private final TableInfo tableInfo;

    private List<MetadataInfo> rowGroupsInfo = Collections.emptyList();
    private List<MetadataInfo> filesInfo = Collections.emptyList();
    private Multimap<Integer, MetadataInfo> segmentsInfo = ArrayListMultimap.create();

    private final List<MetadataInfo> allMetaToHandle = new ArrayList<>();
    private final List<MetadataInfo> metadataToRemove = new ArrayList<>();

    private TableScan tableScan;

    private boolean isChanged = true;

    public FileMetadataInfoCollector(BasicTablesRequests basicRequests, TableInfo tableInfo, FormatSelection selection,
        PlannerSettings settings, Supplier<TableScan> tableScanSupplier, List<SchemaPath> interestingColumns, int segmentColumnsCount) throws IOException {
      this.basicRequests = basicRequests;
      this.tableInfo = tableInfo;
      init(selection, settings, tableScanSupplier, interestingColumns, segmentColumnsCount);
    }

    @Override
    public List<MetadataInfo> getRowGroupsInfo() {
      return rowGroupsInfo;
    }

    @Override
    public List<MetadataInfo> getFilesInfo() {
      return filesInfo;
    }

    @Override
    public Multimap<Integer, MetadataInfo> getSegmentsInfo() {
      return segmentsInfo;
    }

    @Override
    public List<MetadataInfo> getAllMetaToHandle() {
      return allMetaToHandle;
    }

    @Override
    public List<MetadataInfo> getMetadataToRemove() {
      return metadataToRemove;
    }

    @Override
    public TableScan getPrunedScan() {
      return tableScan;
    }

    @Override
    public boolean isChanged() {
      return isChanged;
    }

    private void init(FormatSelection selection, PlannerSettings settings, Supplier<TableScan> tableScanSupplier,
        List<SchemaPath> interestingColumns, int segmentColumnsCount) throws IOException {
      List<SchemaPath> metastoreInterestingColumns =
          Optional.ofNullable(basicRequests.interestingColumnsAndPartitionKeys(tableInfo).interestingColumns())
              .map(metastoreInterestingColumnNames -> metastoreInterestingColumnNames.stream()
                  .map(SchemaPath::parseFromString)
                  .collect(Collectors.toList()))
          .orElse(null);

      Map<String, Long> filesNamesLastModifiedTime = basicRequests.filesLastModifiedTime(tableInfo, null, null);

      List<String> newFiles = new ArrayList<>();
      List<String> updatedFiles = new ArrayList<>();
      List<String> removedFiles = new ArrayList<>(filesNamesLastModifiedTime.keySet());
      List<String> allFiles = new ArrayList<>();

      for (FileStatus fileStatus : getFileStatuses(selection)) {
        // TODO: investigate whether it is possible to store all path attributes. Is it essential?
        String path = Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath()).toUri().getPath();
        Long lastModificationTime = filesNamesLastModifiedTime.get(path);
        if (lastModificationTime == null) {
          newFiles.add(path);
        } else if (lastModificationTime < fileStatus.getModificationTime()) {
          updatedFiles.add(path);
        }
        removedFiles.remove(path);
        allFiles.add(path);
      }

      String selectionRoot = selection.getSelection().getSelectionRoot().toUri().getPath();

      if (!Objects.equals(metastoreInterestingColumns, interestingColumns)
          && (metastoreInterestingColumns == null || !metastoreInterestingColumns.containsAll(interestingColumns))) {
        // do not update table scan and lists of segments / files / row groups,
        // everything should be recalculated
        tableScan = tableScanSupplier.get();
        metadataToRemove.addAll(getMetadataInfoList(selectionRoot, removedFiles, MetadataType.SEGMENT, 0));
        return;
      }

      // checks whether there are no new, updated and removed files
      if (!newFiles.isEmpty() || !updatedFiles.isEmpty() || !removedFiles.isEmpty()) {
        List<String> scanFiles = new ArrayList<>(newFiles);
        scanFiles.addAll(updatedFiles);

        // updates scan to read updated / new files
        tableScan = getTableScan(settings, tableScanSupplier.get(), scanFiles);

        // iterates from the end;
        // takes deepest updated segments,
        // finds their parents:
        //  - fetches all segments for parent level;
        //  - filters segments to leave parents only;
        // obtains all children segments;
        // filters child segments for filtered parent segments

        int lastSegmentIndex = segmentColumnsCount - 1;

        List<String> scanAndRemovedFiles = new ArrayList<>(scanFiles);
        scanAndRemovedFiles.addAll(removedFiles);

        // 1. Obtain files info for files from the same folder without removed files
        // 2. Get segments for obtained files + segments for removed files
        // 3. Get parent segments
        // 4. Get other segments for the same parent segment
        // 5. Remove segments which have only removed files (matched for removedFileInfo and don't match to filesInfo)
        // 6. Do the same for parent segments

        List<MetadataInfo> allFilesInfo = getMetadataInfoList(selectionRoot, allFiles, MetadataType.FILE, 0);

        // first pass: collect updated segments even without files, they will be removed later
        List<MetadataInfo> leafSegments = getMetadataInfoList(selectionRoot, scanAndRemovedFiles, MetadataType.SEGMENT, lastSegmentIndex);
        List<MetadataInfo> removedFilesMetadata = getMetadataInfoList(selectionRoot, removedFiles, MetadataType.FILE, 0);

        List<MetadataInfo> scanFilesInfo = getMetadataInfoList(selectionRoot, scanAndRemovedFiles, MetadataType.FILE, 0);
        // files from scan + files from the same folder without removed files
        filesInfo = leafSegments.stream()
            .filter(parent -> scanFilesInfo.stream().anyMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier())))
            .flatMap(parent ->
                allFilesInfo.stream()
                    .filter(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier())))
            .collect(Collectors.toList());

        Multimap<Integer, MetadataInfo> allSegments = populateSegments(removedFiles, allFiles, selectionRoot, lastSegmentIndex, leafSegments, removedFilesMetadata);

        List<MetadataInfo> allRowGroupsInfo = getAllRowGroupsMetadataInfos(allFiles);

        rowGroupsInfo = allRowGroupsInfo.stream()
            .filter(child -> filesInfo.stream()
                .map(MetadataInfo::identifier)
                .anyMatch(parent -> MetadataIdentifierUtils.isMetadataKeyParent(parent, child.identifier())))
            .collect(Collectors.toList());

        List<MetadataInfo> segmentsToUpdate = getMetadataInfoList(selectionRoot, scanAndRemovedFiles, MetadataType.SEGMENT, 0);
        Streams.concat(allSegments.values().stream(), allFilesInfo.stream(), allRowGroupsInfo.stream())
            .filter(child -> segmentsToUpdate.stream().anyMatch(parent -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier())))
            .filter(parent ->
                removedFilesMetadata.stream().noneMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier()))
                    || filesInfo.stream().anyMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier())))
            .forEach(allMetaToHandle::add);

        allMetaToHandle.addAll(segmentsToUpdate);

        // is handled separately since it is not overridden when writing the metadata
        List<MetadataInfo> removedTopSegments = getMetadataInfoList(selectionRoot, removedFiles, MetadataType.SEGMENT, 0).stream()
            .filter(parent ->
                removedFilesMetadata.stream().anyMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier()))
                    && allFilesInfo.stream().noneMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier())))
            .collect(Collectors.toList());
        metadataToRemove.addAll(removedTopSegments);
      } else {
        // table metadata may still actual
        isChanged = false;
      }
    }

    private Multimap<Integer, MetadataInfo> populateSegments(List<String> removedFiles, List<String> allFiles,
        String selectionRoot, int lastSegmentIndex, List<MetadataInfo> leafSegments, List<MetadataInfo> removedFilesMetadata) {
      List<String> presentAndRemovedFiles = new ArrayList<>(allFiles);
      presentAndRemovedFiles.addAll(removedFiles);
      Multimap<Integer, MetadataInfo> allSegments = ArrayListMultimap.create();
      if (lastSegmentIndex > 0) {
        allSegments.putAll(lastSegmentIndex, getMetadataInfoList(selectionRoot, presentAndRemovedFiles, MetadataType.SEGMENT, lastSegmentIndex));
      }

      for (int i = lastSegmentIndex - 1; i >= 0; i--) {
        List<MetadataInfo> currentChildSegments = leafSegments;
        List<MetadataInfo> allParentSegments = getMetadataInfoList(selectionRoot, presentAndRemovedFiles, MetadataType.SEGMENT, i);
        allSegments.putAll(i, allParentSegments);

        // segments, parent for segments from currentChildSegments
        List<MetadataInfo> parentSegments = allParentSegments.stream()
            .filter(parent -> currentChildSegments.stream().anyMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier())))
            .collect(Collectors.toList());

        // all segments children for parentSegments segments except empty segments
        List<MetadataInfo> childSegments = allSegments.get(i + 1).stream()
            .filter(child -> parentSegments.stream().anyMatch(parent -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier())))
            .filter(parent ->
                removedFilesMetadata.stream().noneMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier()))
                    || filesInfo.stream().anyMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier())))
            .collect(Collectors.toList());

        segmentsInfo.putAll(i + 1, childSegments);
        leafSegments = childSegments;
      }
      segmentsInfo.putAll(0, getMetadataInfoList(selectionRoot, presentAndRemovedFiles, MetadataType.SEGMENT, 0).stream()
          .filter(parent ->
              removedFilesMetadata.stream().noneMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier()))
                  || filesInfo.stream().anyMatch(child -> MetadataIdentifierUtils.isMetadataKeyParent(parent.identifier(), child.identifier())))
          .collect(Collectors.toList()));
      return allSegments;
    }

    private List<MetadataInfo> getAllRowGroupsMetadataInfos(List<String> allFiles) {
      List<String> metadataKeys = filesInfo.stream()
          .map(MetadataInfo::key)
          .distinct()
          .collect(Collectors.toList());

      BasicTablesRequests.RequestMetadata requestMetadata = BasicTablesRequests.RequestMetadata.builder()
          .tableInfo(tableInfo)
          .metadataKeys(metadataKeys)
          .paths(allFiles)
          .metadataType(MetadataType.ROW_GROUP.name())
          .requestColumns(Arrays.asList(MetadataInfo.METADATA_KEY, MetadataInfo.METADATA_IDENTIFIER, MetadataInfo.METADATA_TYPE))
          .build();

      return basicRequests.request(requestMetadata).stream()
          .map(unit -> MetadataInfo.builder().metadataUnit(unit).build())
          .collect(Collectors.toList());
    }

    private List<FileStatus> getFileStatuses(FormatSelection selection) throws IOException {
      FileSelection fileSelection = selection.getSelection();

      if (!fileSelection.isExpandedFully()) {
        fileSelection = getExpandedFileSelection(fileSelection);
      }
      return fileSelection.getFileStatuses();
    }

    private TableScan getTableScan(PlannerSettings settings, TableScan scanRel, List<String> scanFiles) {
      FileSystemPartitionDescriptor descriptor =
          new FileSystemPartitionDescriptor(settings, scanRel);

      List<PartitionLocation> newPartitions = Lists.newArrayList(descriptor.iterator()).stream()
          .flatMap(Collection::stream)
          .flatMap(p -> p.getPartitionLocationRecursive().stream())
          .filter(p -> scanFiles.contains(p.getEntirePartitionLocation().toUri().getPath()))
          .collect(Collectors.toList());

      try {
        if (!newPartitions.isEmpty()) {
          return descriptor.createTableScan(newPartitions, false);
        } else {
          DrillTable drillTable = descriptor.getTable();
          SchemalessScan scan = new SchemalessScan(drillTable.getUserName(), ((FormatSelection) descriptor.getTable().getSelection()).getSelection().getSelectionRoot());

          return new DrillScanRel(scanRel.getCluster(),
              scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              scanRel.getTable(),
              scan,
              scanRel.getRowType(),
              DrillScanRel.getProjectedColumns(scanRel.getTable(), true),
              true /*filter pushdown*/);
        }
      } catch (Exception e) {
        throw new RuntimeException("Error happened during recreation of pruned scan", e);
      }
    }

    private List<MetadataInfo> getMetadataInfoList(String parent, List<String> locations, MetadataType metadataType, int level) {
      return locations.stream()
          .map(location -> getMetadataInfo(parent, location, metadataType, level))
          .distinct()
          .collect(Collectors.toList());
    }

    private MetadataInfo getMetadataInfo(String parent, String location, MetadataType metadataType, int level) {
      List<String> values = ColumnExplorer.listPartitionValues(new Path(location), new Path(parent), true);

      switch (metadataType) {
        case ROW_GROUP: {
          throw new UnsupportedOperationException("MetadataInfo cannot be obtained for row group using file location only");
        }
        case FILE: {
          String key = values.size() > 1 ? values.iterator().next() : MetadataInfo.DEFAULT_SEGMENT_KEY;
          return MetadataInfo.builder()
              .type(metadataType)
              .key(key)
              .identifier(MetadataIdentifierUtils.getMetadataIdentifierKey(values))
              .build();
        }
        case SEGMENT: {
          String key = values.size() > 1 ? values.iterator().next() : MetadataInfo.DEFAULT_SEGMENT_KEY;
          return MetadataInfo.builder()
              .type(metadataType)
              .key(key)
              .identifier(values.size() > 1 ? MetadataIdentifierUtils.getMetadataIdentifierKey(values.subList(0, level + 1)) :  MetadataInfo.DEFAULT_SEGMENT_KEY)
              .build();
        }
        case TABLE: {
          return MetadataInfo.builder()
              .type(metadataType)
              .key(MetadataInfo.GENERAL_INFO_KEY)
              .build();
        }
        default:
          throw new UnsupportedOperationException(metadataType.name());
      }

    }
  }

  public interface AnalyzeInfoProvider {
    List<SchemaPath> getSegmentColumns(DrillTable table, OptionManager options) throws IOException;
    List<SqlIdentifier> getProjectionFields(MetadataType metadataLevel, OptionManager options);

    MetadataInfoCollector getMetadataInfoCollector(BasicTablesRequests basicRequests, TableInfo tableInfo,
        FormatSelection selection, PlannerSettings settings, Supplier<TableScan> scanRel,
        List<SchemaPath> interestingColumns, int segmentColumnsCount) throws IOException;
  }

  private static class AnalyzeFileInfoProvider implements AnalyzeInfoProvider {
    public static final AnalyzeInfoProvider INSTANCE = new AnalyzeFileInfoProvider();

    @Override
    public List<SchemaPath> getSegmentColumns(DrillTable table, OptionManager options) throws IOException {
      FormatSelection selection = (FormatSelection) table.getSelection();

      FileSelection fileSelection = selection.getSelection();
      if (!fileSelection.isExpandedFully()) {
        fileSelection = getExpandedFileSelection(fileSelection);
      }

      return ColumnExplorer.getPartitionColumnNames(fileSelection, options).stream()
          .map(SchemaPath::getSimplePath)
          .collect(Collectors.toList());
    }

    @Override
    public List<SqlIdentifier> getProjectionFields(MetadataType metadataLevel, OptionManager options) {
      List<SqlIdentifier> columnList = new ArrayList<>();
      columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL), SqlParserPos.ZERO));
      if (metadataLevel.compareTo(MetadataType.ROW_GROUP) >= 0) {
        columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL), SqlParserPos.ZERO));
        columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL), SqlParserPos.ZERO));
        columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_ROW_GROUP_LEHGTH_COLUMN_LABEL), SqlParserPos.ZERO));
      }
      columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL), SqlParserPos.ZERO));
      return columnList;
    }

    @Override
    public MetadataInfoCollector getMetadataInfoCollector(BasicTablesRequests basicRequests, TableInfo tableInfo,
        FormatSelection selection, PlannerSettings settings, Supplier<TableScan> tableScanSupplier,
        List<SchemaPath> interestingColumns, int segmentColumnsCount) throws IOException {
      return new FileMetadataInfoCollector(basicRequests, tableInfo, selection,
          settings, tableScanSupplier, interestingColumns, segmentColumnsCount);
    }
  }

  public static class MetadataIdentifierUtils {
    private static final String METADATA_IDENTIFIER_SEPARATOR = "/";

    public static String getMetadataIdentifierKey(List<String> values) {
      return String.join(METADATA_IDENTIFIER_SEPARATOR, values);
    }

    public static boolean isMetadataKeyParent(String parent, String child) {
      return child.startsWith(parent + METADATA_IDENTIFIER_SEPARATOR) || parent.equals(MetadataInfo.DEFAULT_SEGMENT_KEY);
    }

    public static String getFileMetadataIdentifier(List<String> partitionValues, Path path) {
      List<String> identifierValues = new ArrayList<>(partitionValues);
      identifierValues.add(ColumnExplorer.ImplicitFileColumns.FILENAME.getValue(path));
      return getMetadataIdentifierKey(identifierValues);
    }

    public static String getRowGroupMetadataIdentifier(List<String> partitionValues, Path path, int index) {
      List<String> identifierValues = new ArrayList<>(partitionValues);
      identifierValues.add(ColumnExplorer.ImplicitFileColumns.FILENAME.getValue(path));
      identifierValues.add(Integer.toString(index));
      return getMetadataIdentifierKey(identifierValues);
    }

    public static String[] getValuesFromMetadataIdentifier(String metadataIdentifier) {
      return metadataIdentifier.split(METADATA_IDENTIFIER_SEPARATOR);
    }
  }
}
