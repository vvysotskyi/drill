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
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MetadataControllerPOP;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.planner.sql.handlers.MetastoreAnalyzeTableHandler.MetadataIdentifierUtils;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.TupleReader;
import org.apache.drill.metastore.components.tables.TableMetadataUnit;
import org.apache.drill.metastore.components.tables.Tables;
import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.drill.metastore.metadata.BaseMetadata;
import org.apache.drill.metastore.metadata.BaseTableMetadata;
import org.apache.drill.metastore.metadata.FileMetadata;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.PartitionMetadata;
import org.apache.drill.metastore.metadata.RowGroupMetadata;
import org.apache.drill.metastore.metadata.SegmentMetadata;
import org.apache.drill.metastore.metadata.TableInfo;
import org.apache.drill.metastore.operate.Modify;
import org.apache.drill.metastore.statistics.BaseStatisticsKind;
import org.apache.drill.metastore.statistics.ColumnStatistics;
import org.apache.drill.metastore.statistics.ColumnStatisticsKind;
import org.apache.drill.metastore.statistics.ExactStatisticsConstants;
import org.apache.drill.metastore.statistics.StatisticsHolder;
import org.apache.drill.metastore.statistics.StatisticsKind;
import org.apache.drill.metastore.statistics.TableStatisticsKind;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.drill.exec.physical.impl.metadata.MetadataControllerBatch.ColumnNameStatisticsHandler.getStatisticsKind;

public class MetadataControllerBatch extends AbstractSingleRecordBatch<MetadataControllerPOP> {
  private static final Logger logger = LoggerFactory.getLogger(MetadataControllerBatch.class);

  private final Tables tables;
  private final TableInfo tableInfo;
  private final Map<String, MetadataInfo> metadataToHandle;

  protected MetadataControllerBatch(MetadataControllerPOP popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, incoming);
    this.tables = context.getMetastoreRegistry().get().tables();
    this.tableInfo = popConfig.getTableInfo();
    this.metadataToHandle = popConfig.getMetadataToHandle() == null
        ? null
        : popConfig.getMetadataToHandle().stream()
            .collect(Collectors.toMap(MetadataInfo::identifier, Function.identity()));
  }

  @Override
  protected boolean setupNewSchema() {
    container.clear();

    container.addOrGet("ok", Types.required(TypeProtos.MinorType.BIT), null);
    container.addOrGet("Summary", Types.required(TypeProtos.MinorType.VARCHAR), null);

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    return true;
  }

  @Override
  protected IterOutcome doWork() {
    FilterExpression deleteFilter = popConfig.getTableInfo().toFilter();

    for (MetadataInfo metadataInfo : popConfig.getMetadataToRemove()) {
      deleteFilter = FilterExpression.and(deleteFilter,
          FilterExpression.equal(MetadataInfo.METADATA_KEY, metadataInfo.key()));
    }

    Modify<TableMetadataUnit> modify = tables.modify();
    if (!popConfig.getMetadataToRemove().isEmpty()) {
      modify.delete(deleteFilter);
    }
    modify.overwrite(getMetadataUnits(incoming.getContainer()))
        .execute();

    BitVector bitVector = container.addOrGet("ok", Types.required(TypeProtos.MinorType.BIT), null);
    VarCharVector varCharVector = container.addOrGet("Summary", Types.required(TypeProtos.MinorType.VARCHAR), null);

    bitVector.allocateNew();
    varCharVector.allocateNew();

    bitVector.getMutator().set(0, 1);
    varCharVector.getMutator().setSafe(0,
        String.format("Collected / refreshed metadata for table [%s.%s.%s]",
            popConfig.getTableInfo().storagePlugin(),
            popConfig.getTableInfo().workspace(),
            popConfig.getTableInfo().name()).getBytes());

    bitVector.getMutator().setValueCount(1);
    varCharVector.getMutator().setValueCount(1);
    container.setRecordCount(1);

    return IterOutcome.OK;
  }

  private List<TableMetadataUnit> getMetadataUnits(VectorContainer container) {
    List<TableMetadataUnit> metadataUnits = new ArrayList<>();
    RowSetReader reader = DirectRowSet.fromContainer(container).reader();
    while (reader.next()) {
      metadataUnits.addAll(getMetadataUnits(reader, 0));
    }

    if (!metadataToHandle.isEmpty()) {
      // leaves only metadata which belongs to segments be overridden and table metadata
      metadataUnits = metadataUnits.stream()
          .filter(tableMetadataUnit ->
              metadataToHandle.values().stream()
                  .map(MetadataInfo::key)
                  .anyMatch(s -> s.equals(tableMetadataUnit.metadataKey()))
              || MetadataType.TABLE.name().equals(tableMetadataUnit.metadataType()))
          .collect(Collectors.toList());

      // leaves only metadata which should be fetched from the metastore
      metadataUnits.stream()
          .map(TableMetadataUnit::metadataIdentifier)
          .forEach(metadataToHandle::remove);

      List<TableMetadataUnit> metadata = metadataToHandle.isEmpty()
          ? Collections.emptyList()
          : tables.basicRequests().metadata(popConfig.getTableInfo(), metadataToHandle.values());

      metadataUnits.addAll(metadata);
    }

    boolean insertDefaultSegment = metadataUnits.stream()
        .noneMatch(metadataUnit -> metadataUnit.metadataType().equals(MetadataType.SEGMENT.name()));

    if (insertDefaultSegment) {
      TableMetadataUnit defaultSegmentMetadata = getDefaultSegment(metadataUnits);
      metadataUnits.add(defaultSegmentMetadata);
    }

    return metadataUnits;
  }

  private TableMetadataUnit getDefaultSegment(List<TableMetadataUnit> metadataUnits) {
    TableMetadataUnit tableMetadataUnit = metadataUnits.stream()
        .filter(metadataUnit -> metadataUnit.metadataType().equals(MetadataType.TABLE.name()))
        .findAny()
        .orElseThrow(() -> new IllegalStateException("Table metadata wasn't found among collected metadata."));

    List<String> paths = metadataUnits.stream()
        .filter(metadataUnit -> metadataUnit.metadataType().equals(MetadataType.FILE.name()))
        .map(TableMetadataUnit::path)
        .collect(Collectors.toList());

    return tableMetadataUnit.toBuilder()
        .metadataType(MetadataType.SEGMENT.name())
        .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
        .owner(null)
        .tableType(null)
        .metadataStatistics(Collections.emptyList())
        .columnsStatistics(Collections.emptyMap())
        .path(tableMetadataUnit.location())
        .schema(null)
        .locations(paths)
        .build();
  }

  @SuppressWarnings("unchecked")
  private List<TableMetadataUnit> getMetadataUnits(TupleReader reader, int nestingLevel) {
    List<TableMetadataUnit> metadataUnits = new ArrayList<>();
    Multimap<String, StatisticsHolder> columnStatistics = ArrayListMultimap.create();
    List<StatisticsHolder> metadataStatistics = new ArrayList<>();
    Map<String, TypeProtos.MinorType> columnTypes = new HashMap<>();

    TupleMetadata columnMetadata = reader.tupleSchema();
    ObjectReader metadataColumnReader = reader.column(MetadataAggBatch.METADATA_TYPE);
    Preconditions.checkNotNull(metadataColumnReader, "metadataType column wasn't found");

    ObjectReader underlyingMetadataReader = reader.column(MetadataAggBatch.COLLECTED_MAP_FIELD);
    if (underlyingMetadataReader != null) {
      if (!underlyingMetadataReader.schema().isArray()) {
        throw new IllegalStateException("Incoming vector with name `collected_map` should be repeated map");
      }
      // current row contains information about underlying metadata
      ArrayReader array = underlyingMetadataReader.array();
      while (array.next()) {
        metadataUnits.addAll(getMetadataUnits(array.tuple(), nestingLevel + 1));
      }
    }

    for (ColumnMetadata column : columnMetadata) {
      String fieldName = ColumnNameStatisticsHandler.getColumnName(column.name());

      if (ColumnNameStatisticsHandler.columnStatisticsField(column.name())) {
        StatisticsKind statisticsKind = getStatisticsKind(column.name());
        columnStatistics.put(fieldName,
            new StatisticsHolder(getConvertedColumnValue(reader.column(column.name())), statisticsKind));
        if (statisticsKind.getName().equalsIgnoreCase(ColumnStatisticsKind.MIN_VALUE.getName())
            || statisticsKind.getName().equalsIgnoreCase(ColumnStatisticsKind.MAX_VALUE.getName())) {
          columnTypes.putIfAbsent(fieldName, column.type());
        }
      } else if (ColumnNameStatisticsHandler.metadataStatisticsField(column.name())) {
        metadataStatistics.add(new StatisticsHolder(reader.column(column.name()).getObject(), getStatisticsKind(column.name())));
      } else if (column.name().equals(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL))) {
        metadataStatistics.add(new StatisticsHolder(Long.parseLong(reader.column(column.name()).scalar().getString()), new BaseStatisticsKind(ExactStatisticsConstants.START, true)));
      } else if (column.name().equals(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_LEHGTH_COLUMN_LABEL))) {
        metadataStatistics.add(new StatisticsHolder(Long.parseLong(reader.column(column.name()).scalar().getString()), new BaseStatisticsKind(ExactStatisticsConstants.LENGTH, true)));
      }
    }

    Long rowCount = (Long) metadataStatistics.stream()
        .filter(statisticsHolder -> statisticsHolder.getStatisticsKind() == TableStatisticsKind.ROW_COUNT)
        .findAny()
        .map(StatisticsHolder::getStatisticsValue)
        .orElse(null);

    // adds NON_NULL_COUNT to use it during filter pushdown
    if (rowCount != null) {
      Map<String, StatisticsHolder> nullsCountColumnStatistics = new HashMap<>();
      columnStatistics.asMap().forEach((key, value) ->
          value.stream()
              .filter(statisticsHolder -> statisticsHolder.getStatisticsKind() == ColumnStatisticsKind.NON_NULL_COUNT)
              .findAny()
              .map(statisticsHolder -> (Long) statisticsHolder.getStatisticsValue())
              .ifPresent(nonNullCount ->
                  nullsCountColumnStatistics.put(
                      key,
                      new StatisticsHolder(rowCount - nonNullCount, ColumnStatisticsKind.NULLS_COUNT))));

      nullsCountColumnStatistics.forEach(columnStatistics::put);
    }

    Map<SchemaPath, ColumnStatistics> resultingStats = new HashMap<>();

    columnStatistics.asMap().forEach((fieldName, statisticsHolders) ->
        resultingStats.put(SchemaPath.parseFromString(fieldName), new ColumnStatistics(statisticsHolders, columnTypes.get(fieldName))));

    MetadataType metadataType = MetadataType.valueOf(metadataColumnReader.scalar().getString());

    List<String> segmentColumns = popConfig.getSegmentColumns();

    BaseMetadata metadata;

    String lastModifiedTimeCol = context.getOptions().getString(ExecConstants.IMPLICIT_LAST_MODIFIED_TIME_COLUMN_LABEL);
    switch (metadataType) {
      case TABLE: {
        // set storagePlugin, workspace, tableName, owner, tableType - tableInfo
        // metadataType, metadataKey - metadataInfo
        // location, interestingColumns
        // from config
        // get schema, columnsStatistics, metadataStatistics, lastModifiedTime, partitionKeys, additionalMetadata
        // from incoming data and convert / set to table metadata
        MetadataInfo metadataInfo = MetadataInfo.builder()
            .type(MetadataType.TABLE)
            .key(MetadataInfo.GENERAL_INFO_KEY)
            .build();
        metadata = BaseTableMetadata.builder()
            .tableInfo(tableInfo)
            .metadataInfo(metadataInfo)
            .columnsStatistics(resultingStats)
            .metadataStatistics(metadataStatistics)
            .partitionKeys(Collections.emptyMap())
            .interestingColumns(popConfig.getInterestingColumns())
            .location(popConfig.getLocation())
            .lastModifiedTime(Long.parseLong(reader.column(lastModifiedTimeCol).scalar().getString()))
            .schema(TupleMetadata.of(reader.column(MetadataAggBatch.SCHEMA_FIELD).scalar().getString()))
            .build();
        break;
      }
      case SEGMENT: {
        String segmentKey = segmentColumns.size() > 0
            ? reader.column(segmentColumns.iterator().next()).scalar().getString()
            : MetadataInfo.DEFAULT_SEGMENT_KEY;

        List<String> partitionValues = segmentColumns.stream()
            .limit(nestingLevel)
            .map(columnName -> reader.column(columnName).scalar().getString())
            .collect(Collectors.toList());
        String metadataIdentifier = MetadataIdentifierUtils.getMetadataIdentifierKey(partitionValues);
        MetadataInfo metadataInfo = MetadataInfo.builder()
            .type(MetadataType.SEGMENT)
            .key(segmentKey)
            .identifier(StringUtils.defaultIfEmpty(metadataIdentifier, null))
            .build();
        metadata = SegmentMetadata.builder()
            .tableInfo(tableInfo)
            .metadataInfo(metadataInfo)
            .columnsStatistics(resultingStats)
            .metadataStatistics(metadataStatistics)
            .path(new Path(reader.column(MetadataAggBatch.LOCATION_FIELD).scalar().getString()))
            .locations(getIncomingLocations(reader))
            .column(segmentColumns.size() > 0 ? SchemaPath.getSimplePath(segmentColumns.get(nestingLevel - 1)) : null)
            .partitionValues(partitionValues)
            .lastModifiedTime(Long.parseLong(reader.column(lastModifiedTimeCol).scalar().getString()))
            .schema(TupleMetadata.of(reader.column(MetadataAggBatch.SCHEMA_FIELD).scalar().getString()))
            .build();
        break;
      }
      case PARTITION: {
        String segmentKey = segmentColumns.size() > 0
            ? reader.column(segmentColumns.iterator().next()).scalar().getString()
            : MetadataInfo.DEFAULT_SEGMENT_KEY;

        List<String> partitionValues = segmentColumns.stream()
            .limit(nestingLevel)
            .map(columnName -> reader.column(columnName).scalar().getString())
            .collect(Collectors.toList());
        String metadataIdentifier = MetadataIdentifierUtils.getMetadataIdentifierKey(partitionValues);
        MetadataInfo metadataInfo = MetadataInfo.builder()
            .type(MetadataType.PARTITION)
            .key(segmentKey)
            .identifier(StringUtils.defaultIfEmpty(metadataIdentifier, null))
            .build();
        metadata = PartitionMetadata.builder()
            .tableInfo(tableInfo)
            .metadataInfo(metadataInfo)
            .columnsStatistics(resultingStats)
            .metadataStatistics(metadataStatistics)
            .locations(getIncomingLocations(reader))
            .lastModifiedTime(Long.parseLong(reader.column(lastModifiedTimeCol).scalar().getString()))
//            .column(SchemaPath.getSimplePath("dir1"))
//            .partitionValues()
            .schema(TupleMetadata.of(reader.column(MetadataAggBatch.SCHEMA_FIELD).scalar().getString()))
            .build();
        break;
      }
      case FILE: {
        String segmentKey = segmentColumns.size() > 0
            ? reader.column(segmentColumns.iterator().next()).scalar().getString()
            : MetadataInfo.DEFAULT_SEGMENT_KEY;

        List<String> partitionValues = segmentColumns.stream()
            .limit(nestingLevel - 1)
            .map(columnName -> reader.column(columnName).scalar().getString())
            .collect(Collectors.toList());
        Path path = new Path(reader.column(MetadataAggBatch.LOCATION_FIELD).scalar().getString());
        String metadataIdentifier = MetadataIdentifierUtils.getFileMetadataIdentifier(partitionValues, path);
        MetadataInfo metadataInfo = MetadataInfo.builder()
            .type(MetadataType.FILE)
            .key(segmentKey)
            .identifier(StringUtils.defaultIfEmpty(metadataIdentifier, null))
            .build();
        metadata = FileMetadata.builder()
            .tableInfo(tableInfo)
            .metadataInfo(metadataInfo)
            .columnsStatistics(resultingStats)
            .metadataStatistics(metadataStatistics)
            .path(path)
            .lastModifiedTime(Long.parseLong(reader.column(lastModifiedTimeCol).scalar().getString()))
            .schema(TupleMetadata.of(reader.column(MetadataAggBatch.SCHEMA_FIELD).scalar().getString()))
            .build();
        break;
      }
      case ROW_GROUP: {
        String segmentKey = segmentColumns.size() > 0
            ? reader.column(segmentColumns.iterator().next()).scalar().getString()
            : MetadataInfo.DEFAULT_SEGMENT_KEY;

        List<String> partitionValues = segmentColumns.stream()
            .limit(nestingLevel - 2)
            .map(columnName -> reader.column(columnName).scalar().getString())
            .collect(Collectors.toList());
        Path path = new Path(reader.column(MetadataAggBatch.LOCATION_FIELD).scalar().getString());
        int rowGroupIndex = Integer.parseInt(reader.column(context.getOptions().getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL)).scalar().getString());
        String metadataIdentifier = MetadataIdentifierUtils.getRowGroupMetadataIdentifier(partitionValues, path, rowGroupIndex);
        MetadataInfo metadataInfo = MetadataInfo.builder()
            .type(MetadataType.ROW_GROUP)
            .key(segmentKey)
            .identifier(StringUtils.defaultIfEmpty(metadataIdentifier, null))
            .build();
        metadata = RowGroupMetadata.builder()
            .tableInfo(tableInfo)
            .metadataInfo(metadataInfo)
            .columnsStatistics(resultingStats)
            .metadataStatistics(metadataStatistics)
            // TODO: pass host affinity? Am I sure that it is good idea?
            .hostAffinity(Collections.emptyMap())
            .rowGroupIndex(rowGroupIndex)
            .path(path)
            .lastModifiedTime(Long.parseLong(reader.column(lastModifiedTimeCol).scalar().getString()))
            .schema(TupleMetadata.of(reader.column(MetadataAggBatch.SCHEMA_FIELD).scalar().getString()))
            .build();
        break;
      }
      default:
        throw new UnsupportedOperationException("Unsupported metadata type: " + metadataType);
    }
    metadataUnits.add(metadata.toMetadataUnit());

    return metadataUnits;
  }

  private Object getConvertedColumnValue(ObjectReader objectReader) {
    switch (objectReader.schema().type()) {
      case VARBINARY:
      case FIXEDBINARY:
        return new String(objectReader.scalar().getBytes());
      default:
        return objectReader.getObject();
    }

  }

  private Set<Path> getIncomingLocations(TupleReader reader) {
    Set<Path> childLocations = new HashSet<>();

    ObjectReader metadataColumnReader = reader.column(MetadataAggBatch.METADATA_TYPE);
    Preconditions.checkNotNull(metadataColumnReader, "metadataType column wasn't found");

    MetadataType metadataType = MetadataType.valueOf(metadataColumnReader.getObject().toString());

    switch (metadataType) {
      case SEGMENT:
      case PARTITION: {
        ObjectReader locationsReader = reader.column(MetadataAggBatch.LOCATIONS_FIELD);
        // populate list of file locations from "locations" field if it is present in the schema
        if (locationsReader != null && locationsReader.type() == ObjectType.ARRAY) {
          ArrayReader array = locationsReader.array();
          while (array.next()) {
            childLocations.add(new Path(array.scalar().getString()));
          }
          break;
        }
        // in the opposite case, populate list of file locations using underlying metadata
        ObjectReader underlyingMetadataReader = reader.column(MetadataAggBatch.COLLECTED_MAP_FIELD);
        if (underlyingMetadataReader != null) {
          // current row contains information about underlying metadata
          ArrayReader array = underlyingMetadataReader.array();
          array.rewind();
          while (array.next()) {
            childLocations.addAll(getIncomingLocations(array.tuple()));
          }
        }
        break;
      }
      case FILE: {
        childLocations.add(new Path(reader.column(MetadataAggBatch.LOCATION_FIELD).scalar().getString()));
      }
    }

    return childLocations;
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public void dump() {
    logger.error("MetadataHandlerBatch[container={}, popConfig={}]", container, popConfig);
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }

  public static class ColumnNameStatisticsHandler {
    private static final String COLUMN_SEPARATOR = "$";

    public static final Map<StatisticsKind, SqlKind> COLUMN_STATISTICS_FUNCTIONS = ImmutableMap.<StatisticsKind, SqlKind>builder()
        .put(ColumnStatisticsKind.MAX_VALUE, SqlKind.MAX)
        .put(ColumnStatisticsKind.MIN_VALUE, SqlKind.MIN)
        .put(ColumnStatisticsKind.NON_NULL_COUNT, SqlKind.COUNT)
        .put(TableStatisticsKind.ROW_COUNT, SqlKind.COUNT)
        .build();

    public static final Map<StatisticsKind, TypeProtos.MinorType> COLUMN_STATISTICS_TYPES = ImmutableMap.<StatisticsKind, TypeProtos.MinorType>builder()
        .put(ColumnStatisticsKind.NON_NULL_COUNT, TypeProtos.MinorType.BIGINT)
        .put(TableStatisticsKind.ROW_COUNT, TypeProtos.MinorType.BIGINT)
        .build();

    public static final Map<StatisticsKind, SqlKind> META_STATISTICS_FUNCTIONS = ImmutableMap.<StatisticsKind, SqlKind>builder()
        .put(TableStatisticsKind.ROW_COUNT, SqlKind.COUNT)
        .build();

    public static String getColumnName(String fullName) {
      return fullName.substring(fullName.indexOf(COLUMN_SEPARATOR, fullName.indexOf(COLUMN_SEPARATOR) + 1) + 1);
    }

    // TODO: rewrite to cover all known statistics cases and unite logic which assigns names to the columns which logic for parsing field names
    public static StatisticsKind getStatisticsKind(String fullName) {
      String statisticsIdentifier = fullName.split("\\" + COLUMN_SEPARATOR)[1];
      switch (statisticsIdentifier) {
        case "minValue":
          return ColumnStatisticsKind.MIN_VALUE;
        case "maxValue":
          return ColumnStatisticsKind.MAX_VALUE;
        case "nullsCount":
          return ColumnStatisticsKind.NULLS_COUNT;
        case "nonnullrowcount":
          return ColumnStatisticsKind.NON_NULL_COUNT;
        case "rowCount":
          return TableStatisticsKind.ROW_COUNT;
      }
      return new BaseStatisticsKind(statisticsIdentifier, false);
    }

    public static String getColumnStatisticsFieldName(String columnName, StatisticsKind statisticsKind) {
      return String.format("column%1$s%2$s%1$s%3$s", COLUMN_SEPARATOR, statisticsKind.getName(), columnName);
    }

    public static String getMetadataStatisticsFieldName(StatisticsKind statisticsKind) {
      return String.format("metadata%s%s", COLUMN_SEPARATOR, statisticsKind.getName());
    }

    public static boolean columnStatisticsField(String fieldName) {
      return fieldName.startsWith("column" + COLUMN_SEPARATOR);
    }

    public static boolean metadataStatisticsField(String fieldName) {
      return fieldName.startsWith("metadata" + COLUMN_SEPARATOR);
    }
  }
}
