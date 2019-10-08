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

import org.apache.commons.io.FileUtils;
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
import org.apache.drill.metastore.metadata.FileMetadata;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.RowGroupMetadata;
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
import java.util.HashMap;
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

  private static final Map<SchemaPath, ColumnStatistics> TABLE_COLUMN_STATISTICS = ImmutableMap.<SchemaPath, ColumnStatistics>builder()
      .put(SchemaPath.getSimplePath("o_shippriority"),
          getColumnStatistics(0, 0, 120L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_orderstatus"),
          getColumnStatistics("F", "P", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderpriority"),
          getColumnStatistics("1-URGENT", "5-LOW", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderkey"),
          getColumnStatistics(1, 1319, 120L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_clerk"),
          getColumnStatistics("Clerk#000000004", "Clerk#000000995", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_totalprice"),
          getColumnStatistics(3266.69, 350110.21, 120L, TypeProtos.MinorType.FLOAT8))
      .put(SchemaPath.getSimplePath("o_comment"),
          getColumnStatistics(" about the final platelets. dependen",
              "zzle. carefully enticing deposits nag furio", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_custkey"),
          getColumnStatistics(25, 1498, 120L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("dir0"),
          getColumnStatistics("1994", "1996", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("dir1"),
          getColumnStatistics("Q1", "Q4", 120L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderdate"),
          getColumnStatistics(757382400000L, 850953600000L, 120L, TypeProtos.MinorType.DATE))
      .build();

  private static final Map<SchemaPath, ColumnStatistics> DIR0_1994_SEGMENT_COLUMN_STATISTICS = ImmutableMap.<SchemaPath, ColumnStatistics>builder()
      .put(SchemaPath.getSimplePath("o_shippriority"),
          getColumnStatistics(0, 0, 40L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_orderstatus"),
          getColumnStatistics("F", "F", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderpriority"),
          getColumnStatistics("1-URGENT", "5-LOW", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderkey"),
          getColumnStatistics(5, 1031, 40L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_clerk"),
          getColumnStatistics("Clerk#000000004", "Clerk#000000973", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_totalprice"),
          getColumnStatistics(3266.69, 350110.21, 40L, TypeProtos.MinorType.FLOAT8))
      .put(SchemaPath.getSimplePath("o_comment"),
          getColumnStatistics(" accounts nag slyly. ironic, ironic accounts wake blithel",
              "yly final requests over the furiously regula", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_custkey"),
          getColumnStatistics(25, 1469, 40L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("dir0"),
          getColumnStatistics("1994", "1994", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("dir1"),
          getColumnStatistics("Q1", "Q4", 40L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderdate"),
          getColumnStatistics(757382400000L, 788140800000L, 40L, TypeProtos.MinorType.DATE))
      .build();

  private static final Map<SchemaPath, ColumnStatistics> DIR0_1994_Q1_SEGMENT_COLUMN_STATISTICS = ImmutableMap.<SchemaPath, ColumnStatistics>builder()
      .put(SchemaPath.getSimplePath("o_shippriority"),
          getColumnStatistics(0, 0, 10L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_orderstatus"),
          getColumnStatistics("F", "F", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderpriority"),
          getColumnStatistics("1-URGENT", "5-LOW", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderkey"),
          getColumnStatistics(66, 833, 10L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("o_clerk"),
          getColumnStatistics("Clerk#000000062", "Clerk#000000973", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_totalprice"),
          getColumnStatistics(3266.69, 132531.73, 10L, TypeProtos.MinorType.FLOAT8))
      .put(SchemaPath.getSimplePath("o_comment"),
          getColumnStatistics(" special pinto beans use quickly furiously even depende",
              "y pending requests integrate", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_custkey"),
          getColumnStatistics(392, 1411, 10L, TypeProtos.MinorType.INT))
      .put(SchemaPath.getSimplePath("dir0"),
          getColumnStatistics("1994", "1994", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("dir1"),
          getColumnStatistics("Q1", "Q1", 10L, TypeProtos.MinorType.VARCHAR))
      .put(SchemaPath.getSimplePath("o_orderdate"),
          getColumnStatistics(757382400000L, 764640000000L, 10L, TypeProtos.MinorType.DATE))
      .build();

  private static final MetadataInfo TABLE_META_INFO = MetadataInfo.builder()
      .type(MetadataType.TABLE)
      .key(MetadataInfo.GENERAL_INFO_KEY)
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
    queryBuilder().sql("ANALYZE TABLE dfs.`%s` REFRESH METADATA", tableName).run();

    TableInfo tableInfo = getTableInfo(tableName, "default");

    BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
        .basicRequests().tableMetadata(tableInfo);

    Path tablePath = new Path(dirTestWatcher.getRootDir().toURI().getPath(), tableName);

    BaseTableMetadata expectedTableMetadata = getBaseTableMetadata(tableInfo, new File(tablePath.toUri().getPath()), 120L);

    assertEquals(expectedTableMetadata.toMetadataUnit(), actualTableMetadata.toMetadataUnit());

    List<TableMetadataUnit> topSegmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
        .basicRequests()
        .segmentsMetadataByColumn(tableInfo, null, "`dir0`").stream()
        .map(SegmentMetadata::toMetadataUnit)
        .collect(Collectors.toList());

    TableInfo baseTableInfo = TableInfo.builder()
        .name(tableName)
        .storagePlugin("dfs")
        .workspace("default")
        .build();
    TableMetadataUnit dir0 = SegmentMetadata.builder()
        .tableInfo(baseTableInfo)
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.SEGMENT)
            .identifier("1994")
            .key("1994")
            .build())
        .path(new Path(tablePath, "1994"))
        .schema(SCHEMA)
        .lastModifiedTime(new File(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName), "1994").lastModified())
        .column(SchemaPath.getSimplePath("dir0"))
        .columnsStatistics(DIR0_1994_SEGMENT_COLUMN_STATISTICS)
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

    // verify nested segments
    List<TableMetadataUnit> nestedSegmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
        .basicRequests()
        .segmentsMetadataByColumn(tableInfo, null, "`dir1`").stream()
        .map(SegmentMetadata::toMetadataUnit)
        .collect(Collectors.toList());

    assertEquals(12, nestedSegmentMetadata.size());

    TableMetadataUnit dir01994q1Segment = SegmentMetadata.builder()
        .tableInfo(baseTableInfo)
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.SEGMENT)
            .identifier("1994/Q1")
            .key("1994")
            .build())
        .path(new Path(new Path(tablePath, "1994"), "Q1"))
        .schema(SCHEMA)
        .lastModifiedTime(new File(new File(tablePath.toUri().getPath(), "1994"), "Q1").lastModified())
        .column(SchemaPath.getSimplePath("dir1"))
        .columnsStatistics(DIR0_1994_Q1_SEGMENT_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT)))
        .locations(ImmutableSet.of(new Path(tablePath, "1994/Q1/orders_94_q1.parquet")))
        .partitionValues(Arrays.asList("1994", "Q1"))
        .build()
        .toMetadataUnit();

    // verify segment for 1994
    assertEquals(dir01994q1Segment,
        nestedSegmentMetadata.stream()
            .filter(unit -> unit.metadataIdentifier().equals("1994/Q1"))
            .findAny()
            .orElse(null));

    // verify files metadata
    List<TableMetadataUnit> filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
        .basicRequests()
        .filesMetadata(tableInfo, null, null).stream()
        .map(FileMetadata::toMetadataUnit)
        .collect(Collectors.toList());

    assertEquals(12, filesMetadata.size());

    TableMetadataUnit dir01994q1File = FileMetadata.builder()
        .tableInfo(baseTableInfo)
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.FILE)
            .identifier("1994/Q1/orders_94_q1.parquet")
            .key("1994")
            .build())
        .schema(SCHEMA)
        .lastModifiedTime(new File(new File(new File(tablePath.toUri().getPath(), "1994"), "Q1"), "orders_94_q1.parquet").lastModified())
        .columnsStatistics(DIR0_1994_Q1_SEGMENT_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT)))
        .path(new Path(tablePath, "1994/Q1/orders_94_q1.parquet"))
        .build()
        .toMetadataUnit();

    // verify first file metadata
    assertEquals(dir01994q1File,
        filesMetadata.stream()
            .filter(unit -> unit.metadataIdentifier().equals("1994/Q1/orders_94_q1.parquet"))
            .findAny()
            .orElse(null));

    // verify row groups metadata
    List<TableMetadataUnit> rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
        .basicRequests()
        .rowGroupsMetadata(tableInfo, null, (String) null).stream()
        .map(RowGroupMetadata::toMetadataUnit)
        .collect(Collectors.toList());

    assertEquals(12, rowGroupsMetadata.size());

    TableMetadataUnit dir01994q1rowGroup = RowGroupMetadata.builder()
        .tableInfo(baseTableInfo)
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.ROW_GROUP)
            .identifier("1994/Q1/orders_94_q1.parquet/0")
            .key("1994")
            .build())
        .schema(SCHEMA)
        .rowGroupIndex(0)
        .hostAffinity(Collections.emptyMap())
        .lastModifiedTime(new File(new File(new File(tablePath.toUri().getPath(), "1994"), "Q1"), "orders_94_q1.parquet").lastModified())
        .columnsStatistics(DIR0_1994_Q1_SEGMENT_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT)))
        .path(new Path(tablePath, "1994/Q1/orders_94_q1.parquet"))
        .build()
        .toMetadataUnit();

    // verify first row group dir01994q1rowGroup
    assertEquals(dir01994q1rowGroup,
        rowGroupsMetadata.stream()
            .filter(unit -> unit.metadataIdentifier().equals("1994/Q1/orders_94_q1.parquet/0"))
            .findAny()
            .orElse(null));
  }

  @Test
  public void testTableMetadataWithLevels() throws Exception {
    List<String> analyzeLevels =
        Arrays.asList("", "'row_group' level", "'file' level", "'segment' level", "'table' level");

    String tableName = "multilevel/parquet";
    File tablePath = new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName);

    TableInfo tableInfo = getTableInfo(tableName, "default");

    TableMetadataUnit expectedTableMetadata = getBaseTableMetadata(tableInfo, tablePath, 120L).toMetadataUnit();

    for (String analyzeLevel : analyzeLevels) {
      try {
        queryBuilder().sql("ANALYZE TABLE dfs.`%s` REFRESH METADATA %s", tableName, analyzeLevel).run();

        BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
            .basicRequests().tableMetadata(tableInfo);

        assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());
      } finally {
        cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
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

        TableInfo tableInfo = getTableInfo(tableName, "default");

        List<String> emptyMetadataLevels = Arrays.stream(MetadataType.values())
            .filter(metadataType -> metadataType.compareTo(analyzeLevel) > 0
                // for the case when there are no segment metadata, default segment is present
                && metadataType.compareTo(MetadataType.SEGMENT) > 0
                && metadataType.compareTo(MetadataType.ALL) < 0)
            .map(Enum::name)
            .collect(Collectors.toList());

        BasicTablesRequests.RequestMetadata requestMetadata = BasicTablesRequests.RequestMetadata.builder()
            .tableInfo(tableInfo)
            .metadataTypes(emptyMetadataLevels)
            .build();

        List<TableMetadataUnit> metadataUnitList = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
            .read()
            .filter(requestMetadata.filter())
            .execute();
        assertTrue(
            String.format("Some metadata [%s] for [%s] analyze query level is present" + metadataUnitList, emptyMetadataLevels, analyzeLevel),
            metadataUnitList.isEmpty());
      } finally {
        cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
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

    TableInfo tableInfo = getTableInfo(tableName, "default");

    Map<SchemaPath, ColumnStatistics> updatedTableColumnStatistics = new HashMap<>();

    SchemaPath orderStatusPath = SchemaPath.getSimplePath("o_orderstatus");
    SchemaPath dir0Path = SchemaPath.getSimplePath("dir0");
    SchemaPath dir1Path = SchemaPath.getSimplePath("dir1");

    updatedTableColumnStatistics.put(orderStatusPath, TABLE_COLUMN_STATISTICS.get(orderStatusPath));
    updatedTableColumnStatistics.put(dir0Path, TABLE_COLUMN_STATISTICS.get(dir0Path));
    updatedTableColumnStatistics.put(dir1Path, TABLE_COLUMN_STATISTICS.get(dir1Path));

    TableMetadataUnit expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(TupleMetadata.of("{\"type\":\"tuple_schema\",\"columns\":[{\"name\":\"o_orderstatus\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"dir1\",\"type\":\"VARCHAR\",\"mode\":\"OPTIONAL\"}," +
            "{\"name\":\"dir0\",\"type\":\"VARCHAR\",\"mode\":\"OPTIONAL\"}]}"))
        .location(tablePath)
        .columnsStatistics(updatedTableColumnStatistics)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName).lastModified())
        .build()
        .toMetadataUnit();

    try {
      queryBuilder().sql("ANALYZE TABLE dfs.`%s` columns(o_orderstatus) REFRESH METADATA 'row_group' LEVEL", tableName).run();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests().tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();
    }
  }

  @Test
  public void testIncrementalAnalyzeUnchangedTable() throws Exception {
    String tableName = "multilevel/parquet";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    long lastModifiedTime = table.lastModified();

    try {
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, String.format("Collected / refreshed metadata for table [dfs.tmp.%s]", tableName))
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(false, "Analyze is so cool, it knows that table wasn't changed!")
          .go();

      segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      long postAnalyzeLastModifiedTime = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests()
          .metastoreTableInfo(tableInfo)
          .lastModifiedTime();

      assertEquals(lastModifiedTime, postAnalyzeLastModifiedTime);
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalAnalyzeNewParentSegment() throws Exception {
    String tableName = "multilevel/parquet_inc";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    // updates statistics values due to new segment
    Map<SchemaPath, ColumnStatistics> updatedStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
    updatedStatistics.replaceAll((logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Arrays.asList(
                new StatisticsHolder<>(160L, TableStatisticsKind.ROW_COUNT),
                new StatisticsHolder<>(160L, ColumnStatisticsKind.NON_NULL_COUNT)))));

    updatedStatistics.computeIfPresent(SchemaPath.getSimplePath("dir0"), (logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Collections.singletonList(new StatisticsHolder<>("1993", ColumnStatisticsKind.MIN_VALUE)))));

    TableMetadataUnit expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedStatistics)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(160L, TableStatisticsKind.ROW_COUNT)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build()
        .toMetadataUnit();

    try {
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet/1994"), Paths.get(tableName, "1993"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests().tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());

      segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(20, segmentMetadata.size());
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalAnalyzeNewChildSegment() throws Exception {
    String tableName = "multilevel/parquet_inc";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    // updates statistics values due to new segment
    Map<SchemaPath, ColumnStatistics> updatedStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
    updatedStatistics.replaceAll((logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Arrays.asList(
                new StatisticsHolder<>(130L, TableStatisticsKind.ROW_COUNT),
                new StatisticsHolder<>(130L, ColumnStatisticsKind.NON_NULL_COUNT)))));

    updatedStatistics.computeIfPresent(SchemaPath.getSimplePath("dir1"), (logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Collections.singletonList(new StatisticsHolder<>("Q5", ColumnStatisticsKind.MAX_VALUE)))));

    TableMetadataUnit expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedStatistics)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(130L, TableStatisticsKind.ROW_COUNT)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build()
        .toMetadataUnit();

    try {
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel", "parquet", "1994", "Q4"), Paths.get(tableName, "1994", "Q5"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests().tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());

      segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(16, segmentMetadata.size());
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalAnalyzeNewFile() throws Exception {
    String tableName = "multilevel/parquet_inc";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    Path tablePath = new Path(table.toURI().getPath());

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    // updates statistics values due to new segment
    Map<SchemaPath, ColumnStatistics> updatedStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
    updatedStatistics.replaceAll((logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Arrays.asList(
                new StatisticsHolder<>(130L, TableStatisticsKind.ROW_COUNT),
                new StatisticsHolder<>(130L, ColumnStatisticsKind.NON_NULL_COUNT)))));

    TableMetadataUnit expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(tablePath)
        .columnsStatistics(updatedStatistics)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(130L, TableStatisticsKind.ROW_COUNT)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build()
        .toMetadataUnit();

    try {
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      List<SegmentMetadata> segmentsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentsMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());

      dirTestWatcher.copyResourceToTestTmp(
          Paths.get("multilevel", "parquet", "1994", "Q4", "orders_94_q4.parquet"),
          Paths.get(tableName, "1994", "Q4", "orders_94_q4_1.parquet"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests().tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());

      segmentsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      // verifies that segments count left unchanged
      assertEquals(15, segmentsMetadata.size());

      filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(13, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  public void testIncrementalAnalyzeRemovedParentSegment() throws Exception {
    String tableName = "multilevel/parquet_inc";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    TableMetadataUnit expectedTableMetadata = getBaseTableMetadata(tableInfo, table, 120L).toMetadataUnit();

    try {
      dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet/1994"), Paths.get(tableName, "1993"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(20, segmentMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(16, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(16, rowGroupsMetadata.size());

      FileUtils.deleteQuietly(new File(table, "1993"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests().tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());

      segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  public void testIncrementalAnalyzeRemovedNestedSegment() throws Exception {
    String tableName = "multilevel/parquet_inc";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    TableMetadataUnit expectedTableMetadata = getBaseTableMetadata(tableInfo, table, 120L).toMetadataUnit();

    try {
      dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet/1994/Q4"), Paths.get(tableName, "1994", "Q5"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(16, segmentMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(13, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(13, rowGroupsMetadata.size());

      FileUtils.deleteQuietly(new File(new File(table, "1994"), "Q5"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests().tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());

      segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  public void testIncrementalAnalyzeRemovedFile() throws Exception {
    String tableName = "multilevel/parquet_inc";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    TableMetadataUnit expectedTableMetadata = getBaseTableMetadata(tableInfo, table, 120L).toMetadataUnit();

    try {
      dirTestWatcher.copyResourceToTestTmp(
          Paths.get("multilevel", "parquet", "1994", "Q4", "orders_94_q4.parquet"),
          Paths.get(tableName, "1994", "Q4", "orders_94_q4_1.parquet"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(13, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(13, rowGroupsMetadata.size());

      FileUtils.deleteQuietly(new File(new File(new File(table, "1994"), "Q4"), "orders_94_q4_1.parquet"));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests().tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());

      segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testIncrementalAnalyzeUpdatedFile() throws Exception {
    String tableName = "multilevel/parquet_inc";

    File table = dirTestWatcher.copyResourceToTestTmp(Paths.get("multilevel/parquet"), Paths.get(tableName));

    TableInfo tableInfo = getTableInfo(tableName, "tmp");

    try {
      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      List<FileMetadata> filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      List<RowGroupMetadata> rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());

      File fileToUpdate = new File(new File(new File(table, "1994"), "Q4"), "orders_94_q4.parquet");
      long lastModified = fileToUpdate.lastModified();
      FileUtils.deleteQuietly(fileToUpdate);

      // replaces original file
      dirTestWatcher.copyResourceToTestTmp(
          Paths.get("multilevel", "parquet", "1994", "Q1", "orders_94_q1.parquet"),
          Paths.get(tableName, "1994", "Q4", "orders_94_q4.parquet"));

      assertTrue(fileToUpdate.setLastModified(lastModified + 1000));

      testBuilder()
          .sqlQuery("ANALYZE TABLE dfs.tmp.`%s` REFRESH METADATA", tableName)
          .unOrdered()
          .baselineColumns("ok", "Summary")
          .baselineValues(true, "Collected / refreshed metadata for table [dfs.tmp.multilevel/parquet_inc]")
          .go();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests().tableMetadata(tableInfo);

      Map<SchemaPath, ColumnStatistics> tableColumnStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
      tableColumnStatistics.computeIfPresent(SchemaPath.getSimplePath("o_clerk"),
          (logicalExpressions, columnStatistics) ->
              columnStatistics.cloneWith(new ColumnStatistics(
                  Collections.singletonList(new StatisticsHolder<>("Clerk#000000006", ColumnStatisticsKind.MIN_VALUE)))));

      tableColumnStatistics.computeIfPresent(SchemaPath.getSimplePath("o_totalprice"),
          (logicalExpressions, columnStatistics) ->
              columnStatistics.cloneWith(new ColumnStatistics(
                  Collections.singletonList(new StatisticsHolder<>(328207.15, ColumnStatisticsKind.MAX_VALUE)))));

      TableMetadataUnit expectedTableMetadata = BaseTableMetadata.builder()
          .tableInfo(tableInfo)
          .metadataInfo(TABLE_META_INFO)
          .schema(SCHEMA)
          .location(new Path(table.toURI().getPath()))
          .columnsStatistics(tableColumnStatistics)
          .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(120L, TableStatisticsKind.ROW_COUNT)))
          .partitionKeys(Collections.emptyMap())
          .lastModifiedTime(lastModified + 1000)
          .build()
          .toMetadataUnit();

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());

      segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(15, segmentMetadata.size());

      filesMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .filesMetadata(tableInfo, null, null);

      assertEquals(12, filesMetadata.size());

      rowGroupsMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .rowGroupsMetadata(tableInfo, null, (String) null);

      assertEquals(12, rowGroupsMetadata.size());
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();

      FileUtils.deleteQuietly(table);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDefaultSegment() throws Exception {
    String tableName = "multilevel/parquet/1994/Q1";
    Path tablePath = new Path(dirTestWatcher.getRootDir().toURI().getPath(), tableName);

    TableInfo tableInfo = getTableInfo(tableName, "default");

    Map<SchemaPath, ColumnStatistics> tableColumnStatistics = new HashMap<>(TABLE_COLUMN_STATISTICS);
    tableColumnStatistics.remove(SchemaPath.getSimplePath("dir0"));
    tableColumnStatistics.remove(SchemaPath.getSimplePath("dir1"));

    tableColumnStatistics.put(SchemaPath.getSimplePath("o_orderstatus"),
            getColumnStatistics("F", "F", 120L, TypeProtos.MinorType.VARCHAR));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_orderkey"),
            getColumnStatistics(66, 833, 833L, TypeProtos.MinorType.INT));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_clerk"),
            getColumnStatistics("Clerk#000000062", "Clerk#000000973", 120L, TypeProtos.MinorType.VARCHAR));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_totalprice"),
            getColumnStatistics(3266.69, 132531.73, 120L, TypeProtos.MinorType.FLOAT8));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_comment"),
            getColumnStatistics(" special pinto beans use quickly furiously even depende",
                "y pending requests integrate", 120L, TypeProtos.MinorType.VARCHAR));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_custkey"),
            getColumnStatistics(392, 1411, 120L, TypeProtos.MinorType.INT));
    tableColumnStatistics.put(SchemaPath.getSimplePath("o_orderdate"),
            getColumnStatistics(757382400000L, 764640000000L, 120L, TypeProtos.MinorType.DATE));

    tableColumnStatistics.replaceAll((logicalExpressions, columnStatistics) ->
        columnStatistics.cloneWith(new ColumnStatistics(
            Arrays.asList(
                new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT),
                new StatisticsHolder<>(10L, ColumnStatisticsKind.NON_NULL_COUNT)))));

    TableMetadataUnit expectedTableMetadata = BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(TupleMetadata.of("{\"type\":\"tuple_schema\",\"columns\":[" +
            "{\"name\":\"o_orderstatus\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"o_clerk\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"o_orderdate\",\"type\":\"DATE\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"o_shippriority\",\"type\":\"INT\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"o_custkey\",\"type\":\"INT\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"o_totalprice\",\"type\":\"DOUBLE\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"o_orderkey\",\"type\":\"INT\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"o_comment\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}," +
            "{\"name\":\"o_orderpriority\",\"type\":\"VARCHAR\",\"mode\":\"REQUIRED\"}]}"))
        .location(tablePath)
        .columnsStatistics(tableColumnStatistics)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(10L, TableStatisticsKind.ROW_COUNT)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName).lastModified())
        .build()
        .toMetadataUnit();

    TableMetadataUnit defaultSegment = SegmentMetadata.builder()
        .tableInfo(TableInfo.builder()
            .name(tableName)
            .storagePlugin("dfs")
            .workspace("default")
            .build())
        .metadataInfo(MetadataInfo.builder()
            .type(MetadataType.SEGMENT)
            .key(MetadataInfo.DEFAULT_SEGMENT_KEY)
            .build())
        .lastModifiedTime(new File(new File(dirTestWatcher.getRootDir().toURI().getPath(), tableName), "orders_94_q1.parquet").lastModified())
        .columnsStatistics(Collections.emptyMap())
        .metadataStatistics(Collections.emptyList())
        .path(new Path(dirTestWatcher.getRootDir().toURI().getPath(), tableName))
        .locations(ImmutableSet.of(
            new Path(tablePath, "orders_94_q1.parquet")))
        .build()
        .toMetadataUnit();

    try {
      queryBuilder().sql("ANALYZE TABLE dfs.`%s` REFRESH METADATA", tableName).run();

      BaseTableMetadata actualTableMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .basicRequests().tableMetadata(tableInfo);

      assertEquals(expectedTableMetadata, actualTableMetadata.toMetadataUnit());

      List<SegmentMetadata> segmentMetadata = cluster.drillbit().getContext().getMetastoreRegistry().get().tables().basicRequests()
          .segmentsMetadataByMetadataKey(tableInfo, null, null);

      assertEquals(1, segmentMetadata.size());

      assertEquals(defaultSegment, segmentMetadata.get(0).toMetadataUnit());
    } finally {
      cluster.drillbit().getContext().getMetastoreRegistry().get().tables()
          .modify()
          .purge()
          .execute();
    }
  }

  private static <T> ColumnStatistics<T> getColumnStatistics(T minValue, T maxValue, long rowCount, TypeProtos.MinorType minorType) {
    return new ColumnStatistics<>(
        Arrays.asList(
            new StatisticsHolder<>(minValue, ColumnStatisticsKind.MIN_VALUE),
            new StatisticsHolder<>(maxValue, ColumnStatisticsKind.MAX_VALUE),
            new StatisticsHolder<>(rowCount, TableStatisticsKind.ROW_COUNT),
            new StatisticsHolder<>(rowCount, ColumnStatisticsKind.NON_NULL_COUNT)),
        minorType);
  }

  private TableInfo getTableInfo(String tableName, String workspace) {
    return TableInfo.builder()
        .name(tableName)
        .owner(cluster.config().getString("user.name"))
        .storagePlugin("dfs")
        .workspace(workspace)
        .type(TableType.PARQUET.name())
        .build();
  }

  private BaseTableMetadata getBaseTableMetadata(TableInfo tableInfo, File table, long rowCount) {
    return BaseTableMetadata.builder()
        .tableInfo(tableInfo)
        .metadataInfo(TABLE_META_INFO)
        .schema(SCHEMA)
        .location(new Path(table.toURI().getPath()))
        .columnsStatistics(TABLE_COLUMN_STATISTICS)
        .metadataStatistics(Collections.singletonList(new StatisticsHolder<>(rowCount, TableStatisticsKind.ROW_COUNT)))
        .partitionKeys(Collections.emptyMap())
        .lastModifiedTime(table.lastModified())
        .build();
  }
}
