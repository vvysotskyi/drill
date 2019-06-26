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
package org.apache.drill.exec.sql;

import org.apache.drill.categories.MetastoreTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.sql.handlers.MetastoreAnalyzeTableHandler.TableType;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.metastore.components.tables.BasicTablesRequests;
import org.apache.drill.metastore.components.tables.TableMetadataUnit;
import org.apache.drill.metastore.metadata.BaseTableMetadata;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.SegmentMetadata;
import org.apache.drill.metastore.metadata.TableInfo;
import org.apache.drill.metastore.statistics.ColumnStatistics;
import org.apache.drill.metastore.statistics.ColumnStatisticsKind;
import org.apache.drill.metastore.statistics.StatisticsHolder;
import org.apache.drill.metastore.statistics.TableStatisticsKind;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({SlowTest.class, MetastoreTest.class})
public class TestMetastoreCommands extends ClusterTest {

  private static final TupleMetadata SCHEMA = TupleMetadata.of("{\"type\":\"tuple_schema\",\"columns\":[" +
      "{\"name\":\"o_orderstatus\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}," +
      "{\"name\":\"o_clerk\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}," +
      "{\"name\":\"o_orderdate\",\"type\":\"DATE\",\"mode\":\"REQUIRED\"}," +
      "{\"name\":\"o_shippriority\",\"type\":\"INT\",\"mode\":\"REQUIRED\"}," +
      "{\"name\":\"o_custkey\",\"type\":\"INT\",\"mode\":\"REQUIRED\"}," +
      "{\"name\":\"dir1\",\"type\":\"VARCHAR\",\"mode\":\"OPTIONAL\"}," +
      "{\"name\":\"dir0\",\"type\":\"VARCHAR\",\"mode\":\"OPTIONAL\"}," +
      "{\"name\":\"o_totalprice\",\"type\":\"DOUBLE\",\"mode\":\"REQUIRED\"}," +
      "{\"name\":\"o_orderkey\",\"type\":\"INT\",\"mode\":\"REQUIRED\"}," +
      "{\"name\":\"o_comment\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}," +
      "{\"name\":\"o_orderpriority\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}]}");

  private static final ImmutableMap<SchemaPath, ColumnStatistics> TABLE_COLUMN_STATISTICS = ImmutableMap.<SchemaPath, ColumnStatistics>builder()
      .put(SchemaPath.getSimplePath("o_shippriority"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(0, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(0, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_orderstatus"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>("F", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("P", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderpriority"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>("1-URGENT", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("5-LOW", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderkey"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(1, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(1319, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_clerk"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>("Clerk#000000004", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("Clerk#000000995", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_totalprice"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(3266.69, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(350110.21, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.FLOAT8))
      .put(SchemaPath.getSimplePath("o_comment"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(" about the final platelets. dependen", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("zzle. carefully enticing deposits nag furio", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_custkey"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(25, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(1498, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("dir0"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>("1994", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("1996", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("dir1"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>("Q1", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("Q4", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderdate"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(757382400000L, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(850953600000L, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.DATE))
      .build();

  private static final Map<SchemaPath, ColumnStatistics> DIR0_SEGMENT_COLUMN_STATISTICS = ImmutableMap.<SchemaPath, ColumnStatistics>builder()
      .put(SchemaPath.getSimplePath("o_shippriority"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(0, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(0, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_orderstatus"),
          new ColumnStatistics(
              Arrays.asList(
                  new StatisticsHolder<>("F", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("F", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderpriority"),
          new ColumnStatistics(
              Arrays.asList(
                  new StatisticsHolder<>("1-URGENT", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("5-LOW", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderkey"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(5, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(1031, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_clerk"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>("Clerk#000000004", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("Clerk#000000973", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_totalprice"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(3266.69, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(350110.21, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.FLOAT8))
      .put(SchemaPath.getSimplePath("o_comment"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(" accounts nag slyly. ironic, ironic accounts wake blithel", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("yly final requests over the furiously regula", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_custkey"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(25, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(1469, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("dir0"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>("1994", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("1994", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("dir1"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>("Q1", ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>("Q4", ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderdate"),
          new ColumnStatistics<>(
              Arrays.asList(
                  new StatisticsHolder<>(757382400000L, ColumnStatisticsKind.MIN_VALUE),
                  new StatisticsHolder<>(788140800000L, ColumnStatisticsKind.MAX_VALUE),
                  new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT),
                  new StatisticsHolder<>(40L, ColumnStatisticsKind.NON_NULL_COUNT)),
              TypeProtos.MinorType.DATE))
      .build();

  @ClassRule
  public static TemporaryFolder defaultFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUp() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    builder.configProperty(ExecConstants.ZK_ROOT, defaultFolder.getRoot().getPath());
    builder.sessionOption(ExecConstants.METASTORE_ENABLED, true);
    startCluster(builder);

    dirTestWatcher.copyResourceToRoot(Paths.get("multilevel/parquet"));
  }

  @Test
  public void testSimpleAnalyze() throws Exception {
    String tableName = "multilevel/parquet";
    queryBuilder().sql("ANALYZE TABLE dfs.`%s` REFRESH METADATA 'segment' level", tableName).run();

    TableInfo tableInfo = TableInfo.builder()
        .name(tableName)
        .owner(cluster.config().getString("user.name"))
        .storagePlugin("dfs")
        .workspace("default")
        .type(TableType.PARQUET.name())
        .build();

    BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastore().tables()
        .basicRequests().tableMetadata(tableInfo);

    Path tablePath = new Path(dirTestWatcher.getRootDir().toURI().getPath(), tableName);

    BaseTableMetadata expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.TABLE)
            .key(MetadataInfo.GENERAL_INFO_KEY)
            .build())
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(TABLE_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName).lastModified())
        .build();

    assertEquals(expectedTableMetadata.toMetadataUnit(), actualTableMetadata.toMetadataUnit());

    // does not work, uncomment when it is fixed iin Iceberg
//    List<SegmentMetadata> topSegmentMetadata =
//        cluster.drillbit().getContext().getMetastore().tables().basicRequests()
//            .segmentsMetadataByColumn(tableInfo,
//                Arrays.asList(dirTestWatcher.getRootDir().getAbsoluteFile().toURI().getPath() + "/" + tableName + "/1994"),
//                "`dir0`");

    BasicTablesRequests.RequestMetadata requestMetadata = BasicTablesRequests.RequestMetadata.builder()
        .tableInfo(tableInfo)
        .metadataType(MetadataType.SEGMENT.name())
        .requestColumns(TableMetadataUnit.SCHEMA.segmentColumns())
        .build();

    List<TableMetadataUnit> segments = cluster.drillbit().getContext().getMetastore().tables()
        .read()
        .filter(requestMetadata.filter())
        .columns(requestMetadata.columns())
        .execute();

    List<TableMetadataUnit> topSegmentMetadata = segments.stream()
        .filter(unit -> unit.column().equals("`dir0`"))
        .collect(Collectors.toList());

    TableMetadataUnit dir0 = SegmentMetadata.builder()
        .tableInfo(TableInfo.builder()
            .name(tableName)
            .storagePlugin("dfs")
            .workspace("default")
            .build())
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.SEGMENT)
            .identifier("1994")
            .key("1994")
            .build())
        .path(new Path(tablePath, "1994"))
        .schema(SCHEMA)
        .lastModifiedTime(new File(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName), "1994").lastModified())
        .column(SchemaPath.getSimplePath("dir0"))
        .columnsStatistics(DIR0_SEGMENT_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT)))
        .locations(ImmutableSet.of(
            new Path(tablePath, "1994/Q2/orders_94_q2.parquet"),
            new Path(tablePath, "1994/Q4/orders_94_q4.parquet"),
            new Path(tablePath, "1994/Q1/orders_94_q1.parquet"),
            new Path(tablePath, "1994/Q3/orders_94_q3.parquet")))
        .partitionValues(Collections.singletonList("1994"))
        .build()
        .toMetadataUnit();

    // verify segment for 1994
    assertEquals(dir0, topSegmentMetadata.stream().filter(unit -> unit.metadataIdentifier().equals("1994")).findAny().orElse(null));

    List<String> topLevelSegmentLocations = topSegmentMetadata.stream()
        .map(TableMetadataUnit::location)
        .sorted()
        .collect(Collectors.toList());

    List<String> expectedTopLevelSegmentLocations = new ArrayList<>();
    expectedTopLevelSegmentLocations.add(new Path(tablePath, "1994").toUri().getPath());
    expectedTopLevelSegmentLocations.add(new Path(tablePath, "1995").toUri().getPath());
    expectedTopLevelSegmentLocations.add(new Path(tablePath, "1996").toUri().getPath());

    expectedTopLevelSegmentLocations.sort(Comparator.naturalOrder());

    // verify top segments locations
    assertEquals(
        expectedTopLevelSegmentLocations,
        topLevelSegmentLocations);

    List<List<String>> segmentFilesLocations = topSegmentMetadata.stream()
        .map(TableMetadataUnit::locations)
        .peek(list -> list.sort(Comparator.naturalOrder()))
        .sorted(Comparator.comparing(list -> list.iterator().next()))
        .collect(Collectors.toList());

    List<List<String>> expectedSegmentFilesLocations = new ArrayList<>();

    List<String> segmentFiles = new ArrayList<>();

    segmentFiles.add(new Path(tablePath, "1994/Q2/orders_94_q2.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1994/Q4/orders_94_q4.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1994/Q1/orders_94_q1.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1994/Q3/orders_94_q3.parquet").toUri().getPath());
    segmentFiles.sort(Comparator.naturalOrder());
    expectedSegmentFilesLocations.add(segmentFiles);

    segmentFiles = new ArrayList<>();

    segmentFiles.add(new Path(tablePath, "1995/Q2/orders_95_q2.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1995/Q4/orders_95_q4.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1995/Q1/orders_95_q1.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1995/Q3/orders_95_q3.parquet").toUri().getPath());
    segmentFiles.sort(Comparator.naturalOrder());
    expectedSegmentFilesLocations.add(segmentFiles);

    segmentFiles = new ArrayList<>();

    segmentFiles.add(new Path(tablePath, "1996/Q3/orders_96_q3.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1996/Q2/orders_96_q2.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1996/Q4/orders_96_q4.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1996/Q1/orders_96_q1.parquet").toUri().getPath());
    segmentFiles.sort(Comparator.naturalOrder());
    expectedSegmentFilesLocations.add(segmentFiles);

    assertEquals(
        expectedSegmentFilesLocations,
        segmentFilesLocations);

  }

  @Test
  public void testTableMetadataWithLevels() throws Exception {
    List<String> analyzeLevels =
        Arrays.asList("", "'row_group' level", "'file' level", "'segment' level", "'table' level");

    String tableName = "multilevel/parquet";
    Path tablePath = new Path(dirTestWatcher.getRootDir().toURI().getPath(), tableName);

    TableInfo tableInfo = TableInfo.builder()
        .name(tableName)
        .owner(cluster.config().getString("user.name"))
        .storagePlugin("dfs")
        .workspace("default")
        .type(TableType.PARQUET.name())
        .build();

    MetadataInfo metadataInfo = MetadataInfo.builder()
        .type(MetadataType.TABLE)
        .key(MetadataInfo.GENERAL_INFO_KEY)
        .build();

    TableMetadataUnit expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(metadataInfo)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(TABLE_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName).lastModified())
        .build()
        .toMetadataUnit();

    for (String analyzeLevel : analyzeLevels) {
      try {
        queryBuilder().sql("ANALYZE TABLE dfs.`%s` REFRESH METADATA %s", tableName, analyzeLevel).run();

        BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastore().tables()
            .basicRequests().tableMetadata(tableInfo);

        assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());
      } finally {
        cluster.drillbit().getContext().getMetastore().tables()
            .modify()
            .purge()
            .execute();
      }
    }
  }

  @Test
  public void testSegmentMetadataWithLevels() throws Exception {
    List<String> analyzeLevels =
        Arrays.asList("", "'row_group' level", "'file' level",
            "'segment' level");

    String tableName = "multilevel/parquet";
    Path tablePath = new Path(dirTestWatcher.getRootDir().toURI().getPath(), tableName);

    TableInfo tableInfo = TableInfo.builder()
        .name(tableName)
        .owner(cluster.config().getString("user.name"))
        .storagePlugin("dfs")
        .workspace("default")
        .type(TableType.PARQUET.name())
        .build();

    TableMetadataUnit expectedDir01994Metadata = SegmentMetadata.builder()
        .tableInfo(TableInfo.builder()
            .name(tableName)
            .storagePlugin("dfs")
            .workspace("default")
            .build())
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.SEGMENT)
            .identifier("1994")
            .key("1994")
            .build())
        .path(new Path(tablePath, "1994"))
        .schema(SCHEMA)
        .lastModifiedTime(new File(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName), "1994").lastModified())
        .column(SchemaPath.getSimplePath("dir0"))
        .columnsStatistics(DIR0_SEGMENT_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(40L, TableStatisticsKind.ROW_COUNT)))
        .locations(ImmutableSet.of(
            new Path(tablePath, "1994/Q2/orders_94_q2.parquet"),
            new Path(tablePath, "1994/Q4/orders_94_q4.parquet"),
            new Path(tablePath, "1994/Q1/orders_94_q1.parquet"),
            new Path(tablePath, "1994/Q3/orders_94_q3.parquet")))
        .partitionValues(Collections.singletonList("1994"))
        .build()
        .toMetadataUnit();

    List<String> expectedTopLevelSegmentLocations = new ArrayList<>();
    expectedTopLevelSegmentLocations.add(new Path(tablePath, "1994").toUri().getPath());
    expectedTopLevelSegmentLocations.add(new Path(tablePath, "1995").toUri().getPath());
    expectedTopLevelSegmentLocations.add(new Path(tablePath, "1996").toUri().getPath());

    expectedTopLevelSegmentLocations.sort(Comparator.naturalOrder());

    List<List<String>> expectedSegmentFilesLocations = new ArrayList<>();

    List<String> segmentFiles = new ArrayList<>();

    segmentFiles.add(new Path(tablePath, "1994/Q2/orders_94_q2.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1994/Q4/orders_94_q4.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1994/Q1/orders_94_q1.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1994/Q3/orders_94_q3.parquet").toUri().getPath());
    segmentFiles.sort(Comparator.naturalOrder());
    expectedSegmentFilesLocations.add(segmentFiles);

    segmentFiles = new ArrayList<>();

    segmentFiles.add(new Path(tablePath, "1995/Q2/orders_95_q2.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1995/Q4/orders_95_q4.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1995/Q1/orders_95_q1.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1995/Q3/orders_95_q3.parquet").toUri().getPath());
    segmentFiles.sort(Comparator.naturalOrder());
    expectedSegmentFilesLocations.add(segmentFiles);

    segmentFiles = new ArrayList<>();

    segmentFiles.add(new Path(tablePath, "1996/Q3/orders_96_q3.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1996/Q2/orders_96_q2.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1996/Q4/orders_96_q4.parquet").toUri().getPath());
    segmentFiles.add(new Path(tablePath, "1996/Q1/orders_96_q1.parquet").toUri().getPath());
    segmentFiles.sort(Comparator.naturalOrder());
    expectedSegmentFilesLocations.add(segmentFiles);

    for (String analyzeLevel : analyzeLevels) {
      try {
        queryBuilder().sql("ANALYZE TABLE dfs.`%s` REFRESH METADATA %s", tableName, analyzeLevel).run();

        // does not work, uncomment when it is fixed in Iceberg
//    List<SegmentMetadata> topSegmentMetadata =
//        cluster.drillbit().getContext().getMetastore().tables().basicRequests()
//            .segmentsMetadataByColumn(tableInfo,
//                Arrays.asList(dirTestWatcher.getRootDir().getAbsoluteFile().toURI().getPath() + "/" + tableName + "/1994"),
//                "`dir0`");

        BasicTablesRequests.RequestMetadata requestMetadata = BasicTablesRequests.RequestMetadata.builder()
            .tableInfo(tableInfo)
            .metadataType(MetadataType.SEGMENT.name())
            .requestColumns(TableMetadataUnit.SCHEMA.segmentColumns())
            .build();

        List<TableMetadataUnit> segments = cluster.drillbit().getContext().getMetastore().tables()
            .read()
            .filter(requestMetadata.filter())
            .columns(requestMetadata.columns())
            .execute();

        List<TableMetadataUnit> topSegmentMetadata = segments.stream()
            .filter(unit -> unit.column().equals("`dir0`"))
            .collect(Collectors.toList());

        // verify segment for 1994
        TableMetadataUnit actualDir0 = topSegmentMetadata.stream()
            .filter(unit -> unit.metadataIdentifier().equals("1994"))
            .findAny()
            .orElse(null);

        assertEquals(expectedDir01994Metadata, actualDir0);

        List<String> topLevelSegmentLocations = topSegmentMetadata.stream()
            .map(TableMetadataUnit::location)
            .sorted()
            .collect(Collectors.toList());

        // verify top segments locations
        assertEquals(
            expectedTopLevelSegmentLocations,
            topLevelSegmentLocations);

        List<List<String>> segmentFilesLocations = topSegmentMetadata.stream()
            .map(TableMetadataUnit::locations)
            .peek(list -> list.sort(Comparator.naturalOrder()))
            .sorted(Comparator.comparing(list -> list.iterator().next()))
            .collect(Collectors.toList());

        assertEquals(
            expectedSegmentFilesLocations,
            segmentFilesLocations);
      } finally {
        cluster.drillbit().getContext().getMetastore().tables()
            .modify()
            .purge()
            .execute();
      }
    }
  }

  @Test
  public void testAnalyzeLowerLevelMetadata() throws Exception {
    // checks that metadata for levels below specified in analyze statement is absent
    String tableName = "multilevel/parquet";
    List<MetadataType> analyzeLevels =
        Arrays.asList(MetadataType.FILE, MetadataType.SEGMENT, MetadataType.TABLE);

    for (MetadataType analyzeLevel : analyzeLevels) {
      try {
        queryBuilder().sql("ANALYZE TABLE dfs.`%s` REFRESH METADATA '%s' level", tableName, analyzeLevel.name()).run();

        TableInfo tableInfo = TableInfo.builder()
            .name(tableName)
            .owner(cluster.config().getString("user.name"))
            .storagePlugin("dfs")
            .workspace("default")
            .type(TableType.PARQUET.name())
            .build();

        List<String> emptyMetadataLevels = Arrays.stream(MetadataType.values())
            .filter(metadataType -> metadataType.compareTo(analyzeLevel) > 0
                // for the case when there are no segment metadata, default segment is present
//              && metadataType.compareTo(MetadataType.SEGMENT) > 0
                && metadataType.compareTo(MetadataType.ALL) < 0)
            .map(Enum::name)
            .collect(Collectors.toList());

        BasicTablesRequests.RequestMetadata requestMetadata = BasicTablesRequests.RequestMetadata.builder()
            .tableInfo(tableInfo)
            .metadataTypes(emptyMetadataLevels)
            .build();

        List<TableMetadataUnit> metadataUnitList = cluster.drillbit().getContext().getMetastore().tables()
            .read()
            .filter(requestMetadata.filter())
            .execute();
        assertTrue(
            String.format("Some metadata %s for %s analyze query level is present" + metadataUnitList, emptyMetadataLevels, analyzeLevel),
            metadataUnitList.isEmpty());
      } finally {
        cluster.drillbit().getContext().getMetastore().tables()
            .modify()
            .purge()
            .execute();
      }
    }
  }

  @Test
  public void testAnalyzeWithColumns() throws Exception {
    String tableName = "multilevel/parquet";
    Path tablePath = new Path(dirTestWatcher.getRootDir().toURI().getPath(), tableName);

    TableInfo tableInfo = TableInfo.builder()
        .name(tableName)
        .owner(cluster.config().getString("user.name"))
        .storagePlugin("dfs")
        .workspace("default")
        .type(TableType.PARQUET.name())
        .build();

    MetadataInfo metadataInfo = MetadataInfo.builder()
        .type(MetadataType.TABLE)
        .key(MetadataInfo.GENERAL_INFO_KEY)
        .build();

    TableMetadataUnit expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(metadataInfo)
        .schema(TupleMetadata.of("{\"type\":\"tuple_schema\",\"columns\":[{\"name\":\"o_orderstatus\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"dir1\",\"type\":\"VARCHAR\",\"mode\":\"OPTIONAL\"}," +
            "{\"name\":\"dir0\",\"type\":\"VARCHAR\",\"mode\":\"OPTIONAL\"}]}"))
        .location(tablePath)
        .columnsStatistics(ImmutableMap.<SchemaPath, ColumnStatistics>builder()
            .put(SchemaPath.getSimplePath("o_orderstatus"),
                new ColumnStatistics<>(
                    Arrays.asList(
                        new StatisticsHolder<>("F", ColumnStatisticsKind.MIN_VALUE),
                        new StatisticsHolder<>("P", ColumnStatisticsKind.MAX_VALUE),
                        new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                        new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
                    TypeProtos.MinorType.VARCHAR))
            .put(SchemaPath.getSimplePath("dir0"),
                new ColumnStatistics<>(
                    Arrays.asList(
                        new StatisticsHolder<>("1994", ColumnStatisticsKind.MIN_VALUE),
                        new StatisticsHolder<>("1996", ColumnStatisticsKind.MAX_VALUE),
                        new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                        new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
                    TypeProtos.MinorType.VARCHAR))
            .put(SchemaPath.getSimplePath("dir1"),
                new ColumnStatistics<>(
                    Arrays.asList(
                        new StatisticsHolder<>("Q1", ColumnStatisticsKind.MIN_VALUE),
                        new StatisticsHolder<>("Q4", ColumnStatisticsKind.MAX_VALUE),
                        new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT),
                        new StatisticsHolder<>(120L, ColumnStatisticsKind.NON_NULL_COUNT)),
                    TypeProtos.MinorType.VARCHAR))
            .build())
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName).lastModified())
        .build()
        .toMetadataUnit();

    try {
      queryBuilder().sql("ANALYZE TABLE dfs.`%s` columns(o_orderstatus) REFRESH METADATA 'row_group' LEVEL", tableName).run();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastore().tables()
          .basicRequests().tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());
    } finally {
      cluster.drillbit().getContext().getMetastore().tables()
          .modify()
          .purge()
          .execute();
    }
  }

}
