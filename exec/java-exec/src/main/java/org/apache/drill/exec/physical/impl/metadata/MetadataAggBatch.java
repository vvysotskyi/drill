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
package org.apache.drill.exec.physical.impl.metadata;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MetadataAggPOP;
import org.apache.drill.exec.physical.impl.aggregate.StreamingAggBatch;
import org.apache.drill.exec.physical.impl.aggregate.StreamingAggregator;
import org.apache.drill.exec.physical.impl.metadata.MetadataControllerBatch.ColumnNameStatisticsHandler;
import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

public class MetadataAggBatch extends StreamingAggBatch {

  public static final String COLLECTED_MAP_FIELD = "collectedMap";
  // will be populated either in metadata controller or is taken from the metastore
  public static final String LOCATIONS_FIELD = "locations";
  public static final String SCHEMA_FIELD = "schema";
  public static final String METADATA_TYPE = "metadataType";
  public static final String LOCATION_FIELD = "location";

  private List<NamedExpression> valueExpressions;

  public MetadataAggBatch(MetadataAggPOP popConfig, RecordBatch incoming, FragmentContext context) throws OutOfMemoryException {
    super(popConfig, incoming, context);
  }

  @Override
  protected StreamingAggregator createAggregatorInternal()
      throws SchemaChangeException, ClassTransformationException, IOException {
    valueExpressions = new ArrayList<>();
    MetadataAggPOP popConfig = (MetadataAggPOP) this.popConfig;
    List<SchemaPath> interestingColumns = popConfig.getInterestingColumns();

    List<SchemaPath> excludedColumns = popConfig.getExcludedColumns();

    BatchSchema schema = incoming.getSchema();
    // Iterates through input expressions, to add aggregation calls for table fields
    // to collect required statistics (MIN, MAX, COUNT)
    getUnflattenedFileds(Lists.newArrayList(schema), null)
        .forEach((fieldName, fieldRef) -> addColumnAggregateExpressions(fieldRef, fieldName));

    ArrayList<LogicalExpression> fieldsList = new ArrayList<>();
    StreamSupport.stream(schema.spliterator(), false)
        .map(MaterializedField::getName)
        .filter(field -> !excludedColumns.contains(FieldReference.getWithQuotedRef(field)))
        .forEach(filedName -> {
          fieldsList.add(ValueExpressions.getChar(filedName,
              DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM.getDefaultPrecision(SqlTypeName.VARCHAR)));
          fieldsList.add(FieldReference.getWithQuotedRef(filedName));
        });

    if (popConfig.createNewAggregations()) {
      addNewAggregations(fieldsList);
    } else if (!popConfig.createNewAggregations()) {
      // for the case when aggregate on top of non-aggregated data is added, no need to collect raw data into the map
      addAggregationsToCollectAndMergeData(fieldsList);
    }

    for (SchemaPath excludedColumn : excludedColumns) {
      if (excludedColumn.equals(SchemaPath.getSimplePath(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL)))
          || excludedColumn.equals(SchemaPath.getSimplePath(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_LEHGTH_COLUMN_LABEL)))) {
        LogicalExpression lastModifiedTime = new FunctionCall("any_value",
            Collections.singletonList(
                FieldReference.getWithQuotedRef(excludedColumn.getRootSegmentPath())),
            ExpressionPosition.UNKNOWN);

        valueExpressions.add(new NamedExpression(lastModifiedTime,
            FieldReference.getWithQuotedRef(excludedColumn.getRootSegmentPath())));
      }
    }

    addMaxLastModifiedCall();

    return super.createAggregatorInternal();
  }

  private void addMaxLastModifiedCall() {
    String lastModifiedColumn = context.getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL);
    LogicalExpression lastModifiedTime = new FunctionCall("max",
        Collections.singletonList(
            FieldReference.getWithQuotedRef(lastModifiedColumn)),
        ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(lastModifiedTime,
        FieldReference.getWithQuotedRef(lastModifiedColumn)));
  }

  private void addAggregationsToCollectAndMergeData(ArrayList<LogicalExpression> fieldsList) {
    MetadataAggPOP popConfig = (MetadataAggPOP) this.popConfig;
    List<SchemaPath> excludedColumns = popConfig.getExcludedColumns();
    // populate columns which weren't included to the schema, but should be collected to the COLLECTED_MAP_FIELD
    for (SchemaPath logicalExpressions : excludedColumns) {
      fieldsList.add(ValueExpressions.getChar(logicalExpressions.getRootSegmentPath(),
          DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM.getDefaultPrecision(SqlTypeName.VARCHAR)));
      fieldsList.add(FieldReference.getWithQuotedRef(logicalExpressions.getRootSegmentPath()));
    }

    LogicalExpression collectList = new FunctionCall("collect_list",
        fieldsList, ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(collectList, FieldReference.getWithQuotedRef(COLLECTED_MAP_FIELD)));

    // TODO: add function call for merging schemas
    //  Is it enough? perhaps not, it does not resolve schema changes, keep trying better
    LogicalExpression schemaExpr = new FunctionCall("merge_schema",
        Collections.singletonList(FieldReference.getWithQuotedRef(MetadataAggBatch.SCHEMA_FIELD)),
        ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(schemaExpr, FieldReference.getWithQuotedRef(MetadataAggBatch.SCHEMA_FIELD)));
  }

  private void addNewAggregations(List<LogicalExpression> fieldsList) {
    // metadata statistics
    ColumnNameStatisticsHandler.META_STATISTICS_FUNCTIONS.forEach((statisticsKind, sqlKind) -> {
      LogicalExpression call = new FunctionCall(sqlKind.name(),
          Collections.singletonList(ValueExpressions.getBigInt(1)), ExpressionPosition.UNKNOWN);
      valueExpressions.add(
          new NamedExpression(call,
              FieldReference.getWithQuotedRef(ColumnNameStatisticsHandler.getMetadataStatisticsFieldName(statisticsKind))));
    });

    // infer schema from incoming data
    LogicalExpression schemaExpr = new FunctionCall("schema",
        fieldsList, ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(schemaExpr, FieldReference.getWithQuotedRef(MetadataAggBatch.SCHEMA_FIELD)));

    LogicalExpression locationsExpr = new FunctionCall("collect_to_list_varchar",
        Collections.singletonList(SchemaPath.getSimplePath(context.getOptions().getString(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL))), ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(locationsExpr, FieldReference.getWithQuotedRef(MetadataAggBatch.LOCATIONS_FIELD)));
  }

  private Map<String, FieldReference> getUnflattenedFileds(Collection<MaterializedField> fields, List<String> parentFields) {
    Map<String, FieldReference> fieldNameRefMap = new HashMap<>();
    for (MaterializedField field : fields) {
      // statistics collecting is not supported for array types
      if (field.getType().getMode() != TypeProtos.DataMode.REPEATED) {
        MetadataAggPOP popConfig = (MetadataAggPOP) this.popConfig;
        List<SchemaPath> excludedColumns = popConfig.getExcludedColumns();
        // excludedColumns are applied for root fields only
        if (parentFields != null || !excludedColumns.contains(SchemaPath.getSimplePath(field.getName()))) {
          List<String> currentPath;
          if (parentFields == null) {
            currentPath = Collections.singletonList(field.getName());
          } else {
            currentPath = new ArrayList<>(parentFields);
            currentPath.add(field.getName());
          }
          if (field.getType().getMinorType() == TypeProtos.MinorType.MAP && popConfig.createNewAggregations()) {
            fieldNameRefMap.putAll(getUnflattenedFileds(field.getChildren(), currentPath));
          } else {
            SchemaPath schemaPath = SchemaPath.getCompoundPath(currentPath.toArray(new String[0]));
            // adds backticks for popConfig.createNewAggregations() to ensure that field will be parsed correctly
            String name = popConfig.createNewAggregations() ? schemaPath.toExpr() : schemaPath.getRootSegmentPath();
            fieldNameRefMap.put(name, new FieldReference(schemaPath));
          }
        }
      }
    }

    return fieldNameRefMap;
  }

  private void addColumnAggregateExpressions(FieldReference fieldRef, String fieldName) {
    MetadataAggPOP popConfig = (MetadataAggPOP) this.popConfig;
    List<SchemaPath> interestingColumns = popConfig.getInterestingColumns();
    if (popConfig.createNewAggregations()) {
      if (interestingColumns == null || interestingColumns.contains(fieldRef)) {
        // collect statistics for all or only interesting columns if they are specified
        ColumnNameStatisticsHandler.COLUMN_STATISTICS_FUNCTIONS.forEach((statisticsKind, sqlKind) -> {
          LogicalExpression call = new FunctionCall(sqlKind.name(),
              Collections.singletonList(fieldRef), ExpressionPosition.UNKNOWN);
          valueExpressions.add(
              new NamedExpression(call,
                  FieldReference.getWithQuotedRef(ColumnNameStatisticsHandler.getColumnStatisticsFieldName(fieldName, statisticsKind))));
        });
      }
    } else if (ColumnNameStatisticsHandler.columnStatisticsField(fieldName)
        || ColumnNameStatisticsHandler.metadataStatisticsField(fieldName)) {
      SqlKind function = ColumnNameStatisticsHandler.COLUMN_STATISTICS_FUNCTIONS.get(
          ColumnNameStatisticsHandler.getStatisticsKind(fieldName));
      if (function == SqlKind.COUNT) {
        // for the case when aggregation was done, call SUM function for the results of COUNT aggregate call
        function = SqlKind.SUM;
      }
      LogicalExpression functionCall = new FunctionCall(function.name(),
          Collections.singletonList(fieldRef), ExpressionPosition.UNKNOWN);
      valueExpressions.add(new NamedExpression(functionCall, fieldRef));
    }
  }

  @Override
  protected List<NamedExpression> getValueExpressions() {
    return valueExpressions;
  }
}
