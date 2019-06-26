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
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MetadataAggPOP;
import org.apache.drill.exec.physical.impl.aggregate.StreamingAggBatch;
import org.apache.drill.exec.physical.impl.aggregate.StreamingAggregator;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    assert schema != null : "schema is null";
    List<String> fieldNames = new ArrayList<>();
    List<LogicalExpression> fieldRefs = new ArrayList<>();
    // Iterates through input expressions, to add aggregation calls for table fields
    // to collect required statistics (MIN, MAX, COUNT)
    for (MaterializedField materializedField : schema) {
      FieldReference fieldRef = FieldReference.getWithQuotedRef(materializedField.getName());

      String fieldName = removeRenamingPrefix(materializedField.getName());
      if (excludedColumns.contains(fieldRef)) {
        // skip map field with collected statistics, it will be added to the
        continue;
      }
      fieldRefs.add(fieldRef);
      fieldNames.add(fieldName);
      // TODO: unflatten maps to have statistics for nested fields.
      //  Currently, for accessing nested fields, project below is created, but it is possible to walk through all nested fields and add call

      if (popConfig.createNewAggregations()) {
        if (interestingColumns == null || interestingColumns.contains(fieldRef)) {
          // collect statistics for all or only interesting columns if they are specified
          MetadataControllerBatch.ColumnNameStatisticsHandler.COLUMN_STATISTICS_FUNCTIONS.forEach((statisticsKind, sqlKind) -> {
            LogicalExpression call = new FunctionCall(sqlKind.name(),
                Collections.singletonList(fieldRef), ExpressionPosition.UNKNOWN);
            valueExpressions.add(
                new NamedExpression(call,
                    FieldReference.getWithQuotedRef(MetadataControllerBatch.ColumnNameStatisticsHandler.getColumnStatisticsFieldName(fieldName, statisticsKind))));
          });
        }
      } else if (MetadataControllerBatch.ColumnNameStatisticsHandler.columnStatisticsField(fieldName)
          || MetadataControllerBatch.ColumnNameStatisticsHandler.metadataStatisticsField(fieldName)) {
        SqlKind function = MetadataControllerBatch.ColumnNameStatisticsHandler.COLUMN_STATISTICS_FUNCTIONS.get(
            MetadataControllerBatch.ColumnNameStatisticsHandler.getStatisticsKind(fieldName));
        if (function == SqlKind.COUNT) {
          // for the case when aggregation was done, call SUM function for the results of COUNT aggregate call
          function = SqlKind.SUM;
        }
        LogicalExpression functionCall = new FunctionCall(function.name(),
            Collections.singletonList(fieldRef), ExpressionPosition.UNKNOWN);
        valueExpressions.add(new NamedExpression(functionCall, FieldReference.getWithQuotedRef(fieldName)));
      }
    }

    ArrayList<LogicalExpression> fieldsList = new ArrayList<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      fieldsList.add(ValueExpressions.getChar(fieldNames.get(i), DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM.getDefaultPrecision(SqlTypeName.VARCHAR)));
      fieldsList.add(fieldRefs.get(i));
    }

    if (popConfig.createNewAggregations()) {
      // metadata statistics
      MetadataControllerBatch.ColumnNameStatisticsHandler.META_STATISTICS_FUNCTIONS.forEach((statisticsKind, sqlKind) -> {
        LogicalExpression call = new FunctionCall(sqlKind.name(),
            Collections.singletonList(ValueExpressions.getBigInt(1)), ExpressionPosition.UNKNOWN);
        valueExpressions.add(
            new NamedExpression(call,
                FieldReference.getWithQuotedRef(MetadataControllerBatch.ColumnNameStatisticsHandler.getMetadataStatisticsFieldName(statisticsKind))));
      });

      // infer schema from incoming data
      LogicalExpression schemaExpr = new FunctionCall("schema",
          fieldsList, ExpressionPosition.UNKNOWN);

      valueExpressions.add(new NamedExpression(schemaExpr, FieldReference.getWithQuotedRef(MetadataAggBatch.SCHEMA_FIELD)));

      LogicalExpression locationsExpr = new FunctionCall("collect_to_list_varchar",
          Collections.singletonList(SchemaPath.getSimplePath(context.getOptions().getString(ExecConstants.IMPLICIT_FQN_COLUMN_LABEL))), ExpressionPosition.UNKNOWN);

      valueExpressions.add(new NamedExpression(locationsExpr, FieldReference.getWithQuotedRef(MetadataAggBatch.LOCATIONS_FIELD)));
    }

    // for the case when aggregate on top of non-aggregated data is added, no need to collect raw data into the map
    if (!popConfig.createNewAggregations()) {
      // populate columns which weren't included to the schema, but should be collected to the list
      for (SchemaPath logicalExpressions : excludedColumns) {
        fieldsList.add(ValueExpressions.getChar(logicalExpressions.getRootSegmentPath(), DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM.getDefaultPrecision(SqlTypeName.VARCHAR)));
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

    LogicalExpression lastModifiedTime = new FunctionCall("max",
        Collections.singletonList(FieldReference.getWithQuotedRef(context.getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL))), ExpressionPosition.UNKNOWN);

    valueExpressions.add(new NamedExpression(lastModifiedTime, FieldReference.getWithQuotedRef(context.getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL))));

    return super.createAggregatorInternal();
  }

  private String removeRenamingPrefix(String name) {
    String[] nameParts = name.split(StarColumnHelper.PREFIX_DELIMITER);
    return nameParts[nameParts.length - 1];
  }

  @Override
  protected List<NamedExpression> getValueExpressions() {
    return valueExpressions;
  }
}
