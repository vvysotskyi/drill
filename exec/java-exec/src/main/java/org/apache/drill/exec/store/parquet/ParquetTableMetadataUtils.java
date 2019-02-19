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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.exec.store.parquet.metadata.Metadata_V3;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.CollectableColumnStatisticKind;
import org.apache.drill.metastore.ColumnStatistic;
import org.apache.drill.metastore.ColumnStatisticImpl;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.metastore.TableStatistics;
import org.apache.drill.metastore.expr.StatisticsProvider;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.primitives.Longs;
import org.apache.drill.shaded.guava.com.google.common.primitives.UnsignedBytes;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeConstants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for converting parquet metadata classes to metastore metadata classes.
 */
public class ParquetTableMetadataUtils {
  private static final Comparator<byte[]> UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR = Comparator.nullsFirst((b1, b2) ->
      PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(Binary.fromReusedByteArray(b1), Binary.fromReusedByteArray(b2)));

  static final List<CollectableColumnStatisticKind> PARQUET_STATISTICS =
          ImmutableList.of(
              ColumnStatisticsKind.MAX_VALUE,
              ColumnStatisticsKind.MIN_VALUE,
              ColumnStatisticsKind.NULLS_COUNT);

  private ParquetTableMetadataUtils() {
  }

  /**
   * Creates new map based on specified {@code columnStatistics} with added statistics
   * for implicit and partition (dir) columns.
   *
   * @param columnStatistics            map of column statistics to expand
   * @param columns                     list of all columns including implicit or partition ones
   * @param partitionValues             list of partition values
   * @param optionManager               option manager
   * @param location                    location of metadata part
   * @param supportsFileImplicitColumns whether implicit columns are supported
   * @return map with added statistics for implicit and partition (dir) columns
   */
  public static Map<SchemaPath, ColumnStatistic> addImplicitColumnsStatistic(
      Map<SchemaPath, ColumnStatistic> columnStatistics, List<SchemaPath> columns,
      List<String> partitionValues, OptionManager optionManager, String location, boolean supportsFileImplicitColumns) {
    ColumnExplorer columnExplorer = new ColumnExplorer(optionManager, columns);

    Map<String, String> implicitColValues = columnExplorer.populateImplicitColumns(
        location, partitionValues, supportsFileImplicitColumns);
    columnStatistics = new HashMap<>(columnStatistics);
    for (Map.Entry<String, String> partitionValue : implicitColValues.entrySet()) {
      columnStatistics.put(SchemaPath.getCompoundPath(partitionValue.getKey()),
          new StatisticsProvider.MinMaxStatistics<>(partitionValue.getValue(),
              partitionValue.getValue(), Comparator.nullsFirst(Comparator.naturalOrder())));
    }
    return columnStatistics;
  }

  public static List<RowGroupMetadata> getRowGroupsMetadata(MetadataBase.ParquetTableMetadataBase tableMetadata) {
    List<RowGroupMetadata> rowGroups = new ArrayList<>();
    for (MetadataBase.ParquetFileMetadata file : tableMetadata.getFiles()) {
      int index = 0;
      for (MetadataBase.RowGroupMetadata rowGroupMetadata : file.getRowGroups()) {
        rowGroups.add(getRowGroupMetadata(tableMetadata, rowGroupMetadata, index++, file.getPath()));
      }
    }

    return rowGroups;
  }

  public static RowGroupMetadata getRowGroupMetadata(MetadataBase.ParquetTableMetadataBase tableMetadata,
      MetadataBase.RowGroupMetadata rowGroupMetadata, int rgIndexInFile, String location) {
    Map<SchemaPath, ColumnStatistic> columnStatistics = getRowGroupColumnStatistics(tableMetadata, rowGroupMetadata);
    Map<String, Object> rowGroupStatistics = new HashMap<>();
    rowGroupStatistics.put(TableStatistics.ROW_COUNT.getName(), rowGroupMetadata.getRowCount());
    rowGroupStatistics.put("start", rowGroupMetadata.getStart());
    rowGroupStatistics.put("length", rowGroupMetadata.getLength());

    Map<SchemaPath, TypeProtos.MajorType> columns = getRowGroupFields(tableMetadata, rowGroupMetadata);

    TupleSchema schema = new TupleSchema();
    for (Map.Entry<SchemaPath, TypeProtos.MajorType> pathTypePair : columns.entrySet()) {
      SchemaPathUtils.addColumnMetadata(pathTypePair.getKey(), schema, pathTypePair.getValue());
    }

    return new RowGroupMetadata(
      schema, columnStatistics, rowGroupStatistics, rowGroupMetadata.getHostAffinity(), rgIndexInFile, location);
  }

  @SuppressWarnings("unchecked")
  public static <T extends BaseMetadata> Map<SchemaPath, ColumnStatistic> getColumnStatistics(
      List<T> rowGroups, Set<SchemaPath> columns, List<CollectableColumnStatisticKind> statisticsToCollect) {
    Map<SchemaPath, ColumnStatistic> columnStatistics = new HashMap<>();

    for (SchemaPath column : columns) {
      List<ColumnStatistic> statisticsList = new ArrayList<>();
      for (T metadata : rowGroups) {
        ColumnStatistic columnStatistic = metadata.getColumnStatistics().get(column);
        if (columnStatistic == null) {
          // schema change happened, set statistics which represents all nulls
          columnStatistic = new ColumnStatisticImpl(
              ImmutableMap.of(ColumnStatisticsKind.NULLS_COUNT.getName(), metadata.getStatistic(TableStatistics.ROW_COUNT)),
              getNaturalNullsFirstComparator());
        }
        statisticsList.add(columnStatistic);
      }
      Map<String, Object> statisticsMap = new HashMap<>();
      for (CollectableColumnStatisticKind statisticsKind : statisticsToCollect) {
        Object mergedStatistic = statisticsKind.mergeStatistic(statisticsList);
        statisticsMap.put(statisticsKind.getName(), mergedStatistic);
      }
      columnStatistics.put(column, new ColumnStatisticImpl(statisticsMap, statisticsList.iterator().next().getValueComparator()));
    }

    return columnStatistics;
  }

  public static FileMetadata getFileMetadata(List<RowGroupMetadata> rowGroups, String tableName) {
    if (rowGroups.isEmpty()) {
      return null;
    }
    Map<String, Object> fileStatistics = new HashMap<>();
    fileStatistics.put(TableStatistics.ROW_COUNT.getName(), TableStatistics.ROW_COUNT.mergeStatistic(rowGroups));

    TupleSchema schema = rowGroups.iterator().next().getSchema();

    return new FileMetadata(rowGroups.iterator().next().getLocation(), schema,
      getColumnStatistics(rowGroups, rowGroups.iterator().next().getColumnStatistics().keySet(), PARQUET_STATISTICS),
      fileStatistics, tableName, -1);
  }

  public static PartitionMetadata getPartitionMetadata(SchemaPath logicalExpressions, List<FileMetadata> files, String tableName) {
    Set<String> locations = new HashSet<>();
    Set<SchemaPath> columns = new HashSet<>();

    for (FileMetadata file : files) {
      columns.addAll(file.getColumnStatistics().keySet());
      locations.add(file.getLocation());
    }

    Map<String, Object> partStatistics = new HashMap<>();
    partStatistics.put(TableStatistics.ROW_COUNT.getName(), TableStatistics.ROW_COUNT.mergeStatistic(files));

    return new PartitionMetadata(logicalExpressions,
      files.iterator().next().getSchema(), getColumnStatistics(files, columns, PARQUET_STATISTICS),
      partStatistics, locations, tableName, -1);
  }

  public static <T extends Comparable<T>> Comparator<T> getNaturalNullsFirstComparator() {
    return Comparator.nullsFirst(Comparator.naturalOrder());
  }

  @SuppressWarnings("unchecked")
  public static Map<SchemaPath, ColumnStatistic> getRowGroupColumnStatistics(
      MetadataBase.ParquetTableMetadataBase tableMetadata, MetadataBase.RowGroupMetadata rowGroupMetadata) {

    Map<SchemaPath, ColumnStatistic> columnStatistics = new HashMap<>();

    for (MetadataBase.ColumnMetadata column : rowGroupMetadata.getColumns()) {
      SchemaPath colPath = SchemaPath.getCompoundPath(column.getName());

      Long nulls = column.getNulls();
      if (!column.isNumNullsSet() || nulls == null) {
        nulls = GroupScan.NO_COLUMN_STATS;
      }
      PrimitiveType.PrimitiveTypeName primitiveType = getPrimitiveTypeName(tableMetadata, column);
      OriginalType originalType = getOriginalType(tableMetadata, column);
      Comparator comparator = getComparator(primitiveType, originalType);

      Map<String, Object> statistics = new HashMap<>();
      statistics.put(ColumnStatisticsKind.MIN_VALUE.getName(), getValue(column.getMinValue(), primitiveType, originalType));
      statistics.put(ColumnStatisticsKind.MAX_VALUE.getName(), getValue(column.getMaxValue(), primitiveType, originalType));
      statistics.put(ColumnStatisticsKind.NULLS_COUNT.getName(), nulls);
      columnStatistics.put(colPath, new ColumnStatisticImpl(statistics, comparator));
    }

    return columnStatistics;
  }

  public static Object getValue(Object value, PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType) {
    if (value != null) {
      switch (primitiveType) {
        case BOOLEAN:
          return Boolean.parseBoolean(value.toString());

        case INT32:
          if (originalType == OriginalType.DATE) {
            return convertToDrillDateValue(getInt(value));
          } else if (originalType == OriginalType.DECIMAL) {
            return BigInteger.valueOf(getInt(value));
          }
          return getInt(value);

        case INT64:
          if (originalType == OriginalType.DECIMAL) {
            return BigInteger.valueOf(getLong(value));
          } else {
            return getLong(value);
          }

        case FLOAT:
          return getFloat(value);

        case DOUBLE:
          return getDouble(value);

        case INT96:
          return new String(getBytes(value));

        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          if (originalType == OriginalType.DECIMAL) {
            return new BigInteger(getBytes(value));
          } else if (originalType == OriginalType.INTERVAL) {
            return getBytes(value);
          } else {
            return new String(getBytes(value));
          }
      }
    }
    return null;
  }

  private static byte[] getBytes(Object value) {
    if (value instanceof Binary) {
      return ((Binary) value).getBytes();
    } else if (value instanceof byte[]) {
      return (byte[]) value;
    } else if (value instanceof String) { // value is obtained from metadata cache v2+
      return ((String) value).getBytes();
    } else if (value instanceof Map) { // value is obtained from metadata cache v1
      String bytesString = (String) ((Map) value).get("bytes");
      if (bytesString != null) {
        return bytesString.getBytes();
      }
    } else if (value instanceof Long) {
      return Longs.toByteArray((Long) value);
    } else if (value instanceof Integer) {
      return Longs.toByteArray((Integer) value);
    } else if (value instanceof Float) {
      return BigDecimal.valueOf((Float) value).unscaledValue().toByteArray();
    } else if (value instanceof Double) {
      return BigDecimal.valueOf((Double) value).unscaledValue().toByteArray();
    }
    throw new UnsupportedOperationException(String.format("Cannot obtain bytes using value %s", value));
  }

  private static Integer getInt(Object value) {
    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Long) {
      return ((Long) value).intValue();
    } else if (value instanceof Float) {
      return ((Float) value).intValue();
    } else if (value instanceof Double) {
      return ((Double) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt(value.toString());
    } else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).intValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).intValue();
    }
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Long getLong(Object value) {
    if (value instanceof Integer) {
      return Long.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Float) {
      return ((Float) value).longValue();
    } else if (value instanceof Double) {
      return ((Double) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong(value.toString());
    } else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).longValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).longValue();
    }
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Float getFloat(Object value) {
    if (value instanceof Integer) {
      return Float.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return Float.valueOf((Long) value);
    } else if (value instanceof Float) {
      return (Float) value;
    } else if (value instanceof Double) {
      return ((Double) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat(value.toString());
    }
    // TODO: allow conversion form bytes only when actual type of data is known (to obtain scale)
    /* else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).floatValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).floatValue();
    }*/
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Double getDouble(Object value) {
    if (value instanceof Integer) {
      return Double.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return Double.valueOf((Long) value);
    } else if (value instanceof Float) {
      return Double.valueOf((Float) value);
    } else if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof String) {
      return Double.parseDouble(value.toString());
    }
    // TODO: allow conversion form bytes only when actual type of data is known (to obtain scale)
    /* else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).doubleValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).doubleValue();
    }*/
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static long convertToDrillDateValue(int dateValue) {
    return dateValue * (long) DateTimeConstants.MILLIS_PER_DAY;
  }

  public static Comparator getComparator(PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType) {
    if (originalType != null) {
      switch (originalType) {
        case UINT_8:
        case UINT_16:
        case UINT_32:
          return getNaturalNullsFirstComparator();
        case UINT_64:
          return getNaturalNullsFirstComparator();
        case DATE:
        case INT_8:
        case INT_16:
        case INT_32:
        case INT_64:
        case TIME_MICROS:
        case TIME_MILLIS:
        case TIMESTAMP_MICROS:
        case TIMESTAMP_MILLIS:
        case DECIMAL:
        case UTF8:
          return getNaturalNullsFirstComparator();
        case INTERVAL:
          return UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
        default:
          return getNaturalNullsFirstComparator();
      }
    } else {
      switch (primitiveType) {
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case BINARY:
        case INT96:
        case FIXED_LEN_BYTE_ARRAY:
          return getNaturalNullsFirstComparator();
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitiveType);
      }
    }
  }

  public static Comparator getComparator(TypeProtos.MinorType type) {
    switch (type) {
      case INTERVALDAY:
      case INTERVAL:
      case INTERVALYEAR:
        return UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
      case UINT1:
        return Comparator.nullsFirst(UnsignedBytes::compare);
      case UINT2:
        // TODO: check whether it will work for Integer::compareUnsigned
      case UINT4:
        return Comparator.nullsFirst(Integer::compareUnsigned);
      case UINT8:
        return Comparator.nullsFirst(Long::compareUnsigned);
      default:
        return getNaturalNullsFirstComparator();
    }
  }

  static Map<SchemaPath, TypeProtos.MajorType> getFileFields(
    MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ParquetFileMetadata file) {

    // does not resolve types considering all row groups, just takes type from the first row group.
    return getRowGroupFields(parquetTableMetadata, file.getRowGroups().iterator().next());
  }

  public static Map<SchemaPath, TypeProtos.MajorType> getRowGroupFields(
      MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.RowGroupMetadata rowGroup) {
    Map<SchemaPath, TypeProtos.MajorType> columns = new LinkedHashMap<>();
    for (MetadataBase.ColumnMetadata column : rowGroup.getColumns()) {

      PrimitiveType.PrimitiveTypeName primitiveType = getPrimitiveTypeName(parquetTableMetadata, column);
      OriginalType originalType = getOriginalType(parquetTableMetadata, column);
      int precision = 0;
      int scale = 0;
      int definitionLevel = 1;
      int repetitionLevel = 0;
      // only ColumnTypeMetadata_v3 stores information about scale, precision, repetition level and definition level
      if (parquetTableMetadata.hasColumnMetadata() && parquetTableMetadata instanceof Metadata_V3.ParquetTableMetadata_v3) {
        Metadata_V3.ColumnTypeMetadata_v3 columnTypeInfo =
            ((Metadata_V3.ParquetTableMetadata_v3) parquetTableMetadata).getColumnTypeInfo(column.getName());
        scale = columnTypeInfo.scale;
        precision = columnTypeInfo.precision;
        repetitionLevel = parquetTableMetadata.getRepetitionLevel(column.getName());
        definitionLevel = parquetTableMetadata.getDefinitionLevel(column.getName());
      }
      TypeProtos.DataMode mode;
      if (repetitionLevel >= 1) {
        mode = TypeProtos.DataMode.REPEATED;
      } else if (repetitionLevel == 0 && definitionLevel == 0) {
        mode = TypeProtos.DataMode.REQUIRED;
      } else {
        mode = TypeProtos.DataMode.OPTIONAL;
      }
      TypeProtos.MajorType columnType =
          TypeProtos.MajorType.newBuilder(ParquetReaderUtility.getType(primitiveType, originalType, scale, precision))
              .setMode(mode)
              .build();

      SchemaPath columnPath = SchemaPath.getCompoundPath(column.getName());
      TypeProtos.MajorType majorType = columns.get(columnPath);
      if (majorType == null) {
        columns.put(columnPath, columnType);
      } else {
        TypeProtos.MinorType leastRestrictiveType = TypeCastRules.getLeastRestrictiveType(Arrays.asList(majorType.getMinorType(), columnType.getMinorType()));
        if (leastRestrictiveType != majorType.getMinorType()) {
          columns.put(columnPath, columnType);
        }
      }
    }
    return columns;
  }

  public static OriginalType getOriginalType(MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ColumnMetadata column) {
    OriginalType originalType = column.getOriginalType();
    // for the case of parquet metadata v1 version, type information isn't stored in parquetTableMetadata, but in ColumnMetadata
    if (originalType == null) {
      originalType = parquetTableMetadata.getOriginalType(column.getName());
    }
    return originalType;
  }

  public static PrimitiveType.PrimitiveTypeName getPrimitiveTypeName(MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ColumnMetadata column) {
    PrimitiveType.PrimitiveTypeName primitiveType = column.getPrimitiveType();
    // for the case of parquet metadata v1 version, type information isn't stored in parquetTableMetadata, but in ColumnMetadata
    if (primitiveType == null) {
      primitiveType = parquetTableMetadata.getPrimitiveType(column.getName());
    }
    return primitiveType;
  }

  static Map<SchemaPath, TypeProtos.MajorType> resolveFields(MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    LinkedHashMap<SchemaPath, TypeProtos.MajorType> columns = new LinkedHashMap<>();
    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      // row groups in the file have the same schema, so using the first one
      Map<SchemaPath, TypeProtos.MajorType> fileColumns = getFileFields(parquetTableMetadata, file);
      for (Map.Entry<SchemaPath, TypeProtos.MajorType> schemaPathMajorType : fileColumns.entrySet()) {

        SchemaPath columnPath = schemaPathMajorType.getKey();
        TypeProtos.MajorType majorType = columns.get(columnPath);
        if (majorType == null) {
          columns.put(columnPath, schemaPathMajorType.getValue());
        } else {
          TypeProtos.MinorType leastRestrictiveType = TypeCastRules.getLeastRestrictiveType(Arrays.asList(majorType.getMinorType(), schemaPathMajorType.getValue().getMinorType()));
          if (leastRestrictiveType != majorType.getMinorType()) {
            columns.put(columnPath, schemaPathMajorType.getValue());
          }
        }
      }
    }
    return columns;
  }
}
