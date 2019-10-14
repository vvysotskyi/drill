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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MetadataHandlerPOP;
import org.apache.drill.exec.physical.impl.metadata.MetadataControllerBatch.ColumnNameStatisticsHandler;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.OptionBuilder;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.planner.sql.handlers.MetastoreAnalyzeTableHandler.MetadataIdentifierUtils;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.metastore.components.tables.BasicTablesRequests;
import org.apache.drill.metastore.components.tables.Tables;
import org.apache.drill.metastore.metadata.BaseMetadata;
import org.apache.drill.metastore.metadata.FileMetadata;
import org.apache.drill.metastore.metadata.LocationProvider;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.RowGroupMetadata;
import org.apache.drill.metastore.metadata.SegmentMetadata;
import org.apache.drill.metastore.statistics.ExactStatisticsConstants;
import org.apache.drill.metastore.statistics.StatisticsKind;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.NONE;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.STOP;

public class MetadataHandlerBatch extends AbstractSingleRecordBatch<MetadataHandlerPOP> {
  private static final Logger logger = LoggerFactory.getLogger(MetadataHandlerBatch.class);

  private final Tables tables;
  private final MetadataType metadataType;
  private final Map<String, MetadataInfo> metadataToHandle;
  private boolean firstBatch = true;

  protected MetadataHandlerBatch(MetadataHandlerPOP popConfig,
      FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, incoming);
    this.tables = context.getMetastoreRegistry().get().tables();
    this.metadataType = popConfig.getMetadataHandlerContext().metadataType();
    this.metadataToHandle = popConfig.getMetadataHandlerContext().metadataToHandle() != null
        ? popConfig.getMetadataHandlerContext().metadataToHandle().stream()
            .collect(Collectors.toMap(MetadataInfo::identifier, Function.identity()))
        : null;
  }

  @Override
  public IterOutcome doWork() {
    // 1. Consume data from incoming operators and update metadataToHandle to remove incoming metadata
    // 2. For the case when incoming operator returned nothing - no updated underlying metadata was found.
    // 3. Fetches metadata which should be handled but wasn't returned by incoming batch from the metastore

    IterOutcome outcome = next(incoming);

    switch (outcome) {
      case NONE:
        if (firstBatch) {
          Preconditions.checkState(metadataToHandle.isEmpty(),
              "Incoming batch didn't return the result for modified segments");
        }
        return outcome;
      case OK_NEW_SCHEMA:
        if (firstBatch) {
          firstBatch = false;
          if (!setupNewSchema()) {
            outcome = IterOutcome.OK;
          }
        }
        doWorkInternal();
        return outcome;
      case OK:
        assert !firstBatch : "First batch should be OK_NEW_SCHEMA";
        doWorkInternal();
        // fall thru
      case OUT_OF_MEMORY:
      case NOT_YET:
      case STOP:
        return outcome;
      default:
        throw new UnsupportedOperationException("Unsupported upstream state " + outcome);
    }
  }

  @Override
  public IterOutcome innerNext() {
    IterOutcome outcome = getLastKnownOutcome();
    if (outcome != NONE && outcome != STOP) {
      outcome = super.innerNext();
    }
    if (outcome == IterOutcome.NONE && !metadataToHandle.isEmpty()) {
      BasicTablesRequests basicTablesRequests = tables.basicRequests();

      switch (metadataType) {
        case ROW_GROUP: {
          List<RowGroupMetadata> rowGroups =
              basicTablesRequests.rowGroupsMetadata(popConfig.getMetadataHandlerContext().tableInfo(), new ArrayList<>(metadataToHandle.values()));
          return populateContainer(rowGroups);
        }
        case FILE: {
          List<FileMetadata> files =
              basicTablesRequests.filesMetadata(popConfig.getMetadataHandlerContext().tableInfo(), new ArrayList<>(metadataToHandle.values()));
          return populateContainer(files);
        }
        case SEGMENT: {
          List<SegmentMetadata> segments =
              basicTablesRequests.segmentsMetadata(popConfig.getMetadataHandlerContext().tableInfo(), new ArrayList<>(metadataToHandle.values()));
          return populateContainer(segments);
        }
      }
    }
    return outcome;
  }

  private <T extends BaseMetadata & LocationProvider> IterOutcome populateContainer(List<T> metadata) {
    VectorContainer populatedContainer;
    if (firstBatch) {
      populatedContainer = writeMetadata(metadata);
      setupSchemaFromContainer(populatedContainer);
    } else {
      populatedContainer = writeMetadataUsingBatchSchema(metadata);
    }
    container.transferIn(populatedContainer);
    container.setRecordCount(populatedContainer.getRecordCount());

    if (firstBatch) {
      firstBatch = false;
      return IterOutcome.OK_NEW_SCHEMA;
    } else {
      return IterOutcome.OK;
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends BaseMetadata & LocationProvider> VectorContainer writeMetadata(List<T> metadataList) {
    BaseMetadata firstElement = metadataList.iterator().next();

    ResultSetLoader resultSetLoader = getResultSetLoaderForMetadata(firstElement);
    resultSetLoader.startBatch();
    RowSetLoader rowWriter = resultSetLoader.writer();
    Iterator<T> segmentsIterator = metadataList.iterator();
    while (!rowWriter.isFull() && segmentsIterator.hasNext()) {
      T metadata = segmentsIterator.next();
      metadataToHandle.remove(metadata.getMetadataInfo().identifier());

      List<Object> arguments = new ArrayList<>();
      arguments.add(metadata.getPath().toUri().getPath());
      Collections.addAll(
          arguments,
          Arrays.copyOf(MetadataIdentifierUtils.getValuesFromMetadataIdentifier(metadata.getMetadataInfo().identifier()), popConfig.getMetadataHandlerContext().segmentColumns().size()));

      metadata.getColumnsStatistics().entrySet().stream()
          .sorted(Comparator.comparing(e -> e.getKey().getRootSegmentPath()))
          .map(Map.Entry::getValue)
          .flatMap(columnStatistics ->
              ColumnNameStatisticsHandler.COLUMN_STATISTICS_FUNCTIONS.keySet().stream()
                  .map(columnStatistics::get))
          .forEach(arguments::add);

      ColumnNameStatisticsHandler.META_STATISTICS_FUNCTIONS.keySet().stream()
          .map(metadata::getStatistic)
          .forEach(arguments::add);

      // current code checks whether this field exists, so do not create it if we don't want to populate it???
      arguments.add(null);

      if (metadataType == MetadataType.SEGMENT) {
        arguments.add(((SegmentMetadata) metadata).getLocations().stream()
            .map(path -> path.toUri().getPath())
            .toArray(String[]::new));
      }

      if (metadataType == MetadataType.ROW_GROUP) {
        arguments.add(Long.toString(((RowGroupMetadata) metadata).getRowGroupIndex()));
        arguments.add(Long.toString(metadata.getStatistic(() -> ExactStatisticsConstants.START)));
        arguments.add(Long.toString(metadata.getStatistic(() -> ExactStatisticsConstants.START)));
      }

      arguments.add(metadata.getSchema().jsonString());
      arguments.add(Long.toString(metadata.getLastModifiedTime()));
      arguments.add(metadataType.name());
      rowWriter.addRow(arguments.toArray());
    }

    return resultSetLoader.harvest();
  }

  private ResultSetLoader getResultSetLoaderForMetadata(BaseMetadata baseMetadata) {
    SchemaBuilder schemaBuilder = new SchemaBuilder()
        .addNullable(MetadataAggBatch.LOCATION_FIELD, MinorType.VARCHAR);
    for (String segmentColumn : popConfig.getMetadataHandlerContext().segmentColumns()) {
      schemaBuilder.addNullable(segmentColumn, MinorType.VARCHAR);
    }

    baseMetadata.getColumnsStatistics().entrySet().stream()
        .sorted(Comparator.comparing(e -> e.getKey().getRootSegmentPath()))
        // TODO: replace with stream API if possible...
        .forEach(entry -> {
          for (StatisticsKind statisticsKind : ColumnNameStatisticsHandler.COLUMN_STATISTICS_FUNCTIONS.keySet()) {
            MinorType type = ColumnNameStatisticsHandler.COLUMN_STATISTICS_TYPES.get(statisticsKind);
            type = type != null ? type : entry.getValue().getComparatorType();
            schemaBuilder.addNullable(
                ColumnNameStatisticsHandler.getColumnStatisticsFieldName(entry.getKey().getRootSegmentPath(), statisticsKind),
                type);
          }
        });

    for (StatisticsKind statisticsKind : ColumnNameStatisticsHandler.META_STATISTICS_FUNCTIONS.keySet()) {
      schemaBuilder.addNullable(
          ColumnNameStatisticsHandler.getMetadataStatisticsFieldName(statisticsKind),
          ColumnNameStatisticsHandler.COLUMN_STATISTICS_TYPES.get(statisticsKind));
    }

    // current code checks whether this field exists, so do not create it if we don't want to populate it???
    schemaBuilder
        .addMapArray(MetadataAggBatch.COLLECTED_MAP_FIELD)
        .resumeSchema();

    if (metadataType == MetadataType.SEGMENT) {
      schemaBuilder.addArray(MetadataAggBatch.LOCATIONS_FIELD, MinorType.VARCHAR);
    }

    if (metadataType == MetadataType.ROW_GROUP) {
      schemaBuilder.addNullable(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL), MinorType.VARCHAR);
      schemaBuilder.addNullable(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL), MinorType.VARCHAR);
      schemaBuilder.addNullable(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_LEHGTH_COLUMN_LABEL), MinorType.VARCHAR);
    }

    schemaBuilder
        .addNullable(MetadataAggBatch.SCHEMA_FIELD, MinorType.VARCHAR)
        .addNullable(context.getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL), MinorType.VARCHAR)
        .add(MetadataAggBatch.METADATA_TYPE, MinorType.VARCHAR);

    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schemaBuilder.buildSchema())
        .build();

    return new ResultSetLoaderImpl(container.getAllocator(), options);
  }

  @SuppressWarnings("unchecked")
  private <T extends BaseMetadata & LocationProvider> VectorContainer writeMetadataUsingBatchSchema(List<T> segments) {
    Preconditions.checkState(segments.size() > 0, "Unable to fetch segments.");

    String lastModifiedTimeField = context.getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL);
    String rgiField = context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL);
    String rgsField = context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL);
    String rglField = context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_LEHGTH_COLUMN_LABEL);

    ResultSetLoader resultSetLoader = getResultSetLoaderWithBatchSchema();
    resultSetLoader.startBatch();
    RowSetLoader rowWriter = resultSetLoader.writer();
    Iterator<T> segmentsIterator = segments.iterator();
    while (!rowWriter.isFull() && segmentsIterator.hasNext()) {
      T metadata = segmentsIterator.next();
      metadataToHandle.remove(metadata.getMetadataInfo().identifier());

      List<Object> arguments = new ArrayList<>();
      for (VectorWrapper<?> vectorWrapper : container) {

        String[] identifierValues = Arrays.copyOf(MetadataIdentifierUtils.getValuesFromMetadataIdentifier(metadata.getMetadataInfo().identifier()), popConfig.getMetadataHandlerContext().segmentColumns().size());

        // TODO: too ugly, find the way to rewrite it without if-else chain
        MaterializedField field = vectorWrapper.getField();
        String fieldName = field.getName();
        if (fieldName.equals(MetadataAggBatch.LOCATION_FIELD)) {
          arguments.add(metadata.getPath().toUri().getPath());
        } else if (fieldName.equals(MetadataAggBatch.LOCATIONS_FIELD)) {
          if (metadataType == MetadataType.SEGMENT) {
            arguments.add(((SegmentMetadata) metadata).getLocations().stream()
                .map(path -> path.toUri().getPath())
                .toArray(String[]::new));
          } else {
            arguments.add(null);
          }
        } else if (popConfig.getMetadataHandlerContext().segmentColumns().contains(fieldName)) {
          arguments.add(identifierValues[popConfig.getMetadataHandlerContext().segmentColumns().indexOf(fieldName)]);
        } else if (ColumnNameStatisticsHandler.columnStatisticsField(fieldName)) {
          arguments.add(
              metadata.getColumnStatistics(SchemaPath.parseFromString(ColumnNameStatisticsHandler.getColumnName(fieldName)))
                  .get(ColumnNameStatisticsHandler.getStatisticsKind(fieldName)));
        } else if (ColumnNameStatisticsHandler.metadataStatisticsField(fieldName)) {
          arguments.add(metadata.getStatistic(ColumnNameStatisticsHandler.getStatisticsKind(fieldName)));
        } else if (fieldName.equals(MetadataAggBatch.COLLECTED_MAP_FIELD)) {
          // current code checks whether this field exists, so do not create it if we don't want to populate it???
          arguments.add(null);
        } else if (fieldName.equals(MetadataAggBatch.SCHEMA_FIELD)) {
          arguments.add(metadata.getSchema().jsonString());
        } else if (fieldName.equals(lastModifiedTimeField)) {
          arguments.add(Long.toString(metadata.getLastModifiedTime()));
        } else if (fieldName.equals(rgiField)) {
          arguments.add(Long.toString(((RowGroupMetadata) metadata).getRowGroupIndex()));
        } else if (fieldName.equals(rgsField)) {
          arguments.add(Long.toString(metadata.getStatistic(() -> ExactStatisticsConstants.START)));
        } else if (fieldName.equals(rglField)) {
          arguments.add(Long.toString(metadata.getStatistic(() -> ExactStatisticsConstants.LENGTH)));
        } else if (fieldName.equals(MetadataAggBatch.METADATA_TYPE)) {
          arguments.add(metadataType.name());
        } else {
          throw new UnsupportedOperationException(String.format("Found unexpected field [%s] in incoming batch.",  field));
        }
      }

      rowWriter.addRow(arguments.toArray());
    }

    return resultSetLoader.harvest();
  }

  private ResultSetLoader getResultSetLoaderWithBatchSchema() {
    String lastModifiedTimeField = context.getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL);
    String rgiField = context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL);
    String rgsField = context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL);
    String rglField = context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_LEHGTH_COLUMN_LABEL);
    SchemaBuilder schemaBuilder = new SchemaBuilder();
    // adds fields to the schema preserving their order to avoid issues in outcoming batches
    for (VectorWrapper<?> vectorWrapper : container) {
      MaterializedField field = vectorWrapper.getField();
      String fieldName = field.getName();
      if (fieldName.equals(MetadataAggBatch.LOCATION_FIELD)
          || fieldName.equals(MetadataAggBatch.SCHEMA_FIELD)
          || fieldName.equals(lastModifiedTimeField)
          || fieldName.equals(rgiField)
          || fieldName.equals(rgsField)
          || fieldName.equals(rglField)
          || fieldName.equals(MetadataAggBatch.METADATA_TYPE)
          || popConfig.getMetadataHandlerContext().segmentColumns().contains(fieldName)) {
        schemaBuilder.add(fieldName, field.getType().getMinorType(), field.getDataMode());
      } else if (ColumnNameStatisticsHandler.columnStatisticsField(fieldName)
          || ColumnNameStatisticsHandler.metadataStatisticsField(fieldName)) {
        schemaBuilder.add(fieldName, field.getType().getMinorType(), field.getType().getMode());
      } else if (fieldName.equals(MetadataAggBatch.COLLECTED_MAP_FIELD)) {
        schemaBuilder.addMapArray(fieldName)
            .resumeSchema();
      } else if (fieldName.equals(MetadataAggBatch.LOCATIONS_FIELD)) {
        schemaBuilder.addArray(fieldName, MinorType.VARCHAR);
      } else {
        throw new UnsupportedOperationException(String.format("Found unexpected field [%s] in incoming batch.",  field));
      }
    }

    ResultSetLoaderImpl.ResultSetOptions options = new OptionBuilder()
        .setSchema(schemaBuilder.buildSchema())
        .build();

    return new ResultSetLoaderImpl(container.getAllocator(), options);
  }

  private void setupSchemaFromContainer(VectorContainer populatedContainer) {
    container.clear();
    StreamSupport.stream(populatedContainer.spliterator(), false)
        .map(VectorWrapper::getField)
        .filter(field -> field.getType().getMinorType() != MinorType.NULL)
        .forEach(container::addOrGet);
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
  }

  protected boolean setupNewSchema() {
    setupSchemaFromContainer(incoming.getContainer());
    return true;
  }

  protected IterOutcome doWorkInternal() {
    container.transferIn(incoming.getContainer());
    VarCharVector valueVector = container.addOrGet(
        MaterializedField.create(MetadataAggBatch.METADATA_TYPE, Types.required(MinorType.VARCHAR)));
    valueVector.allocateNew();
    // TODO: replace with adequate solution
    for (int i = 0; i < incoming.getRecordCount(); i++) {
      valueVector.getMutator().setSafe(i, metadataType.name().getBytes());
    }

    valueVector.getMutator().setValueCount(incoming.getRecordCount());
    container.setRecordCount(incoming.getRecordCount());

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    updateMetadataToHandle();

    return IterOutcome.OK;
  }

  private void updateMetadataToHandle() {
    RowSetReader reader = DirectRowSet.fromContainer(container).reader();
    // updates metadataToHandle to be able to fetch required data which wasn't returned by incoming batch
    if (metadataToHandle != null && !metadataToHandle.isEmpty()) {
      switch (metadataType) {
        case ROW_GROUP: {
          String rgiColumnName = context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL);
          while (reader.next() && !metadataToHandle.isEmpty()) {
            List<String> partitionValues = popConfig.getMetadataHandlerContext().segmentColumns().stream()
                .map(columnName -> reader.column(columnName).scalar().getString())
                .collect(Collectors.toList());
            Path location = new Path(reader.column(MetadataAggBatch.LOCATION_FIELD).scalar().getString());
            int rgi = Integer.parseInt(reader.column(rgiColumnName).scalar().getString());
            metadataToHandle.remove(MetadataIdentifierUtils.getRowGroupMetadataIdentifier(partitionValues, location, rgi));
          }
          break;
        }
        case FILE: {
          while (reader.next() && !metadataToHandle.isEmpty()) {
            List<String> partitionValues = popConfig.getMetadataHandlerContext().segmentColumns().stream()
                .map(columnName -> reader.column(columnName).scalar().getString())
                .collect(Collectors.toList());
            Path location = new Path(reader.column(MetadataAggBatch.LOCATION_FIELD).scalar().getString());
            // use metadata identifier for files since row group indexes are not required when file is updated
            metadataToHandle.remove(MetadataIdentifierUtils.getFileMetadataIdentifier(partitionValues, location));
          }
          break;
        }
        case SEGMENT: {
          while (reader.next() && !metadataToHandle.isEmpty()) {
            List<String> partitionValues = popConfig.getMetadataHandlerContext().segmentColumns().stream()
                .limit(popConfig.getMetadataHandlerContext().depthLevel())
                .map(columnName -> reader.column(columnName).scalar().getString())
                .collect(Collectors.toList());
            metadataToHandle.remove(MetadataIdentifierUtils.getMetadataIdentifierKey(partitionValues));
          }
          break;
        }
      }
    }
  }

  @Override
  public void dump() {
    logger.error("MetadataHandlerBatch[container={}, popConfig={}]", container, popConfig);
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }
}
