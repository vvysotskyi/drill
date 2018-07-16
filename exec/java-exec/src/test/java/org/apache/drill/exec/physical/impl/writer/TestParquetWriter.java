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
package org.apache.drill.exec.physical.impl.writer;

import static org.apache.drill.exec.store.parquet.ParquetRecordWriter.DRILL_VERSION_PROPERTY;
import static org.apache.drill.test.TestBuilder.convertToLocalDateTime;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.util.Pair;
import org.apache.drill.categories.ParquetTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.fn.interp.TestConstantFolding;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.test.BaseTestQuery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
@Category({SlowTest.class, ParquetTest.class})
public class TestParquetWriter extends BaseTestQuery {
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { {100} });
  }

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("parquet", "int96_dict_change"));
  }

  private static FileSystem fs;

  // Map storing a convenient name as well as the cast type necessary
  // to produce it casting from a varchar
  private static final Map<String, String> allTypes = new HashMap<>();

  // Select statement for all supported Drill types, for use in conjunction with
  // the file parquet/alltypes.json in the resources directory
  private static final String allTypesSelection;

  static {
    allTypes.put("int",                "int");
    allTypes.put("bigint",             "bigint");
    allTypes.put("decimal(9, 4)",      "decimal9");
    allTypes.put("decimal(18,9)",      "decimal18");
    allTypes.put("decimal(28, 14)",    "decimal28sparse");
    allTypes.put("decimal(38, 19)",    "decimal38sparse");
    allTypes.put("decimal(38, 15)",    "vardecimal");
    allTypes.put("date",               "date");
    allTypes.put("timestamp",          "timestamp");
    allTypes.put("float",              "float4");
    allTypes.put("double",             "float8");
    allTypes.put("varbinary(65000)",   "varbinary");
    // TODO(DRILL-2297)
//    allTypes.put("interval year",      "intervalyear");
    allTypes.put("interval day",       "intervalday");
    allTypes.put("boolean",            "bit");
    allTypes.put("varchar",            "varchar");
    allTypes.put("time",               "time");

    List<String> allTypeSelectsAndCasts = new ArrayList<>();
    for (String s : allTypes.keySet()) {
      // don't need to cast a varchar, just add the column reference
      if (s.equals("varchar")) {
        allTypeSelectsAndCasts.add(String.format("`%s_col`", allTypes.get(s)));
        continue;
      }
      allTypeSelectsAndCasts.add(String.format("cast(`%s_col` AS %S) `%s_col`", allTypes.get(s), s, allTypes.get(s)));
    }
    allTypesSelection = String.join(",", allTypeSelectsAndCasts);
  }

  private final String allTypesTable = "cp.`parquet/alltypes.json`";

  @Parameterized.Parameter
  public int repeat = 1;

  @BeforeClass
  public static void initFs() throws Exception {
    fs = getLocalFileSystem();
    alterSession(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true);
  }

  @AfterClass
  public static void disableDecimalDataType() {
    resetSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
  }

  @Test
  public void testSmallFileValueReadWrite() throws Exception {
    String selection = "key";
    String inputTable = "cp.`store/json/intData.json`";
    runTestAndValidate(selection, selection, inputTable, "smallFileTest");
  }

  @Test
  public void testSimple() throws Exception {
    String selection = "*";
    String inputTable = "cp.`employee.json`";
    runTestAndValidate(selection, selection, inputTable, "employee_parquet");
  }

  @Test
  public void testLargeFooter() throws Exception {
    StringBuilder sb = new StringBuilder();
    // create a JSON document with a lot of columns
    sb.append("{");
    final int numCols = 1000;
    String[] colNames = new String[numCols];
    Object[] values = new Object[numCols];
    for (int i = 0 ; i < numCols - 1; i++) {
      sb.append(String.format("\"col_%d\" : 100,", i));
      colNames[i] = "col_" + i;
      values[i] = 100L;
    }
    // add one column without a comma after it
    sb.append(String.format("\"col_%d\" : 100", numCols - 1));
    sb.append("}");
    colNames[numCols - 1] = "col_" + (numCols - 1);
    values[numCols - 1] = 100L;

    String path = "test";
    File pathDir = dirTestWatcher.makeRootSubDir(Paths.get(path));

    // write it to a file in the temp directory for the test
    new TestConstantFolding.SmallFileCreator(pathDir)
      .setRecord(sb.toString()).createFiles(1, 1, "json");

    test("use dfs.tmp");
    test("create table WIDE_PARQUET_TABLE_TestParquetWriter_testLargeFooter as select * from dfs.`%s/smallfile/smallfile.json`", path);
    testBuilder()
        .sqlQuery("select * from dfs.tmp.WIDE_PARQUET_TABLE_TestParquetWriter_testLargeFooter")
        .unOrdered()
        .baselineColumns(colNames)
        .baselineValues(values)
        .build().run();
  }

  @Test
  public void testAllScalarTypes() throws Exception {
    /// read once with the flat reader
    runTestAndValidate(allTypesSelection, "*", allTypesTable, "donuts_json");

    try {
      // read all of the types with the complex reader
      alterSession(ExecConstants.PARQUET_NEW_RECORD_READER, true);
      runTestAndValidate(allTypesSelection, "*", allTypesTable, "donuts_json");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_NEW_RECORD_READER);
    }
  }

  @Test
  public void testAllScalarTypesDictionary() throws Exception {
    try {
      alterSession(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, true);
      /// read once with the flat reader
      runTestAndValidate(allTypesSelection, "*", allTypesTable, "donuts_json");

      // read all of the types with the complex reader
      alterSession(ExecConstants.PARQUET_NEW_RECORD_READER, true);
      runTestAndValidate(allTypesSelection, "*", allTypesTable, "donuts_json");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_NEW_RECORD_READER);
      resetSessionOption(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING);
    }
  }

  @Test
  public void testDictionaryError() throws Exception {
    compareParquetReadersColumnar("*", "cp.`parquet/required_dictionary.parquet`");
    runTestAndValidate("*", "*", "cp.`parquet/required_dictionary.parquet`", "required_dictionary");
  }

  @Test
  public void testDictionaryEncoding() throws Exception {
    String selection = "type";
    String inputTable = "cp.`donuts.json`";
    try {
      alterSession(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, true);
      runTestAndValidate(selection, selection, inputTable, "donuts_json");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING);
    }
  }

  @Test
  public void testComplex() throws Exception {
    String selection = "*";
    String inputTable = "cp.`donuts.json`";
    runTestAndValidate(selection, selection, inputTable, "donuts_json");
  }

  @Test
  public void testComplexRepeated() throws Exception {
    String selection = "*";
    String inputTable = "cp.`testRepeatedWrite.json`";
    runTestAndValidate(selection, selection, inputTable, "repeated_json");
  }

  @Test
  public void testCastProjectBug_Drill_929() throws Exception {
    String selection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, cast(L_COMMITDATE as DATE) as COMMITDATE, cast(L_RECEIPTDATE as DATE) AS RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
    String validationSelection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE,COMMITDATE ,RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";

    String inputTable = "cp.`tpch/lineitem.parquet`";
    runTestAndValidate(selection, validationSelection, inputTable, "drill_929");
}

  @Test
  public void testTPCHReadWrite1() throws Exception {
    String inputTable = "cp.`tpch/lineitem.parquet`";
    runTestAndValidate("*", "*", inputTable, "lineitem_parquet_all");
  }

  @Test
  public void testTPCHReadWrite1_date_convertedType() throws Exception {
    try {
      alterSession(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, false);
      String selection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, cast(L_COMMITDATE as DATE) as L_COMMITDATE, cast(L_RECEIPTDATE as DATE) AS L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
      String validationSelection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE,L_COMMITDATE ,L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
      String inputTable = "cp.`tpch/lineitem.parquet`";
      runTestAndValidate(selection, validationSelection, inputTable, "lineitem_parquet_converted");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING);
    }
  }

  @Test
  public void testTPCHReadWrite2() throws Exception {
    String inputTable = "cp.`tpch/customer.parquet`";
    runTestAndValidate("*", "*", inputTable, "customer_parquet");
  }

  @Test
  public void testTPCHReadWrite3() throws Exception {
    String inputTable = "cp.`tpch/nation.parquet`";
    runTestAndValidate("*", "*", inputTable, "nation_parquet");
  }

  @Test
  public void testTPCHReadWrite4() throws Exception {
    String inputTable = "cp.`tpch/orders.parquet`";
    runTestAndValidate("*", "*", inputTable, "orders_parquet");
  }

  @Test
  public void testTPCHReadWrite5() throws Exception {
    String inputTable = "cp.`tpch/part.parquet`";
    runTestAndValidate("*", "*", inputTable, "part_parquet");
  }

  @Test
  public void testTPCHReadWrite6() throws Exception {
    String inputTable = "cp.`tpch/partsupp.parquet`";
    runTestAndValidate("*", "*", inputTable, "partsupp_parquet");
  }

  @Test
  public void testTPCHReadWrite7() throws Exception {
    String inputTable = "cp.`tpch/region.parquet`";
    runTestAndValidate("*", "*", inputTable, "region_parquet");
  }

  @Test
  public void testTPCHReadWrite8() throws Exception {
    String inputTable = "cp.`tpch/supplier.parquet`";
    runTestAndValidate("*", "*", inputTable, "supplier_parquet");
  }

  @Test
  public void testTPCHReadWriteNoDictUncompressed() throws Exception {
    try {
      alterSession(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING, false);
      alterSession(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE, "none");
      String inputTable = "cp.`tpch/supplier.parquet`";
      runTestAndValidate("*", "*", inputTable, "supplier_parquet_no_dict_uncompressed");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING);
      resetSessionOption(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE);
    }
  }

  @Test
  public void testTPCHReadWriteDictGzip() throws Exception {
    try {
      alterSession(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE, "gzip");
      String inputTable = "cp.`tpch/supplier.parquet`";
      runTestAndValidate("*", "*", inputTable, "supplier_parquet_dict_gzip");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE);
    }
  }

  // working to create an exhaustive test of the format for this one. including all convertedTypes
  // will not be supporting interval for Beta as of current schedule
  // Types left out:
  // "TIMESTAMPTZ_col"
  @Test
  public void testRepeated() throws Exception {
    String inputTable = "cp.`parquet/basic_repeated.json`";
    runTestAndValidate("*", "*", inputTable, "basic_repeated");
  }

  @Test
  public void testRepeatedDouble() throws Exception {
    String inputTable = "cp.`parquet/repeated_double_data.json`";
    runTestAndValidate("*", "*", inputTable, "repeated_double_parquet");
  }

  @Test
  public void testRepeatedLong() throws Exception {
    String inputTable = "cp.`parquet/repeated_integer_data.json`";
    runTestAndValidate("*", "*", inputTable, "repeated_int_parquet");
  }

  @Test
  public void testRepeatedBool() throws Exception {
    String inputTable = "cp.`parquet/repeated_bool_data.json`";
    runTestAndValidate("*", "*", inputTable, "repeated_bool_parquet");
  }

  @Test
  public void testNullReadWrite() throws Exception {
    String inputTable = "cp.`parquet/null_test_data.json`";
    runTestAndValidate("*", "*", inputTable, "nullable_test");
  }

  @Ignore("Test file not available")
  @Test
  public void testBitError_Drill_2031() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`tmp/wide2/0_0_3.parquet`");
  }

  @Test
  public void testDecimal() throws Exception {
    String selection = "cast(salary as decimal(8,2)) as decimal8, cast(salary as decimal(15,2)) as decimal15, " +
        "cast(salary as decimal(24,2)) as decimal24, cast(salary as decimal(38,2)) as decimal38";
    String validateSelection = "decimal8, decimal15, decimal24, decimal38";
    String inputTable = "cp.`employee.json`";

    // DRILL-5833: The "old" writer had a decimal bug, but the new one
    // did not. The one used was random. Force the test to run both
    // the old and new readers.

    try {
      alterSession(ExecConstants.PARQUET_NEW_RECORD_READER, true);
      runTestAndValidate(selection, validateSelection, inputTable, "parquet_decimal");
      alterSession(ExecConstants.PARQUET_NEW_RECORD_READER, false);
      runTestAndValidate(selection, validateSelection, inputTable, "parquet_decimal");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_NEW_RECORD_READER);
    }
  }

  @Test
  public void testMulipleRowGroups() throws Exception {
    try {
      alterSession(ExecConstants.PARQUET_BLOCK_SIZE, 1024*1024);
      String selection = "mi";
      String inputTable = "cp.`customer.json`";
      runTestAndValidate(selection, selection, inputTable, "foodmart_customer_parquet");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_BLOCK_SIZE);
    }
  }

  @Test
  public void testDate() throws Exception {
    String selection = "cast(hire_date as DATE) as hire_date";
    String validateSelection = "hire_date";
    String inputTable = "cp.`employee.json`";
    runTestAndValidate(selection, validateSelection, inputTable, "foodmart_employee_parquet");
  }

  @Test
  public void testBoolean() throws Exception {
    String selection = "true as x, false as y";
    String validateSelection = "x, y";
    String inputTable = "cp.`tpch/region.parquet`";
    runTestAndValidate(selection, validateSelection, inputTable, "region_boolean_parquet");
  }

  @Test //DRILL-2030
  public void testWriterWithStarAndExp() throws Exception {
    String selection = " *, r_regionkey + 1 r_regionkey2";
    String validateSelection = "r_regionkey, r_name, r_comment, r_regionkey + 1 r_regionkey2";
    String inputTable = "cp.`tpch/region.parquet`";
    runTestAndValidate(selection, validateSelection, inputTable, "region_star_exp");
  }

  @Test // DRILL-2458
  public void testWriterWithStarAndRegluarCol() throws Exception {
    String outputFile = "region_sort";
    String ctasStmt = "create table " + outputFile + " as select *, r_regionkey + 1 as key1 from cp.`tpch/region.parquet` order by r_name";
    String query = "select r_regionkey, r_name, r_comment, r_regionkey +1 as key1 from cp.`tpch/region.parquet` order by r_name";
    String queryFromWriteOut = "select * from " + outputFile;

    try {
      test("use dfs.tmp");
      test(ctasStmt);
      testBuilder()
          .ordered()
          .sqlQuery(queryFromWriteOut)
          .sqlBaselineQuery(query)
          .build().run();
    } finally {
      deleteTableIfExists(outputFile);
    }
  }

  public void compareParquetReadersColumnar(String selection, String table) throws Exception {
    String query = "select " + selection + " from " + table;

    try {
      testBuilder()
        .ordered()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery("alter system set `store.parquet.use_new_reader` = false")
        .sqlBaselineQuery(query)
        .optionSettingQueriesForBaseline("alter system set `store.parquet.use_new_reader` = true")
        .build().run();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_NEW_RECORD_READER);
    }
  }

  public void compareParquetReadersHyperVector(String selection, String table) throws Exception {

    String query = "select " + selection + " from " + table;
    try {
      testBuilder()
        .ordered()
        .highPerformanceComparison()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery(
            "alter system set `store.parquet.use_new_reader` = false")
        .sqlBaselineQuery(query)
        .optionSettingQueriesForBaseline(
            "alter system set `store.parquet.use_new_reader` = true")
        .build().run();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_NEW_RECORD_READER);
    }
  }

  @Ignore("Binary file too large for version control")
  @Test
  public void testReadVoter() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`tmp/voter.parquet`");
  }

  @Ignore("Test file not available")
  @Test
  public void testReadSf_100_supplier() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`tmp/sf100_supplier.parquet`");
  }

  @Ignore("Binary file too large for version control")
  @Test
  public void testParquetRead_checkNulls_NullsFirst() throws Exception {
    compareParquetReadersColumnar("*",
        "dfs.`tmp/parquet_with_nulls_should_sum_100000_nulls_first.parquet`");
  }

  @Ignore("Test file not available")
  @Test
  public void testParquetRead_checkNulls() throws Exception {
    compareParquetReadersColumnar("*", "dfs.`tmp/parquet_with_nulls_should_sum_100000.parquet`");
  }

  @Ignore("Binary file too large for version control")
  @Test
  public void test958_sql() throws Exception {
    compareParquetReadersHyperVector("ss_ext_sales_price", "dfs.`tmp/store_sales`");
  }

  @Ignore("Binary file too large for version control")
  @Test
  public void testReadSf_1_supplier() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`tmp/orders_part-m-00001.parquet`");
  }

  @Ignore("Binary file too large for version control")
  @Test
  public void test958_sql_all_columns() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`tmp/store_sales`");
    compareParquetReadersHyperVector("ss_addr_sk, ss_hdemo_sk", "dfs.`tmp/store_sales`");
    // TODO - Drill 1388 - this currently fails, but it is an issue with project, not the reader, pulled out the physical plan
    // removed the unneeded project in the plan and ran it against both readers, they outputs matched
//    compareParquetReadersHyperVector("pig_schema,ss_sold_date_sk,ss_item_sk,ss_cdemo_sk,ss_addr_sk, ss_hdemo_sk",
//        "dfs.`tmp/store_sales`");
  }

  @Ignore("Binary file too large for version control")
  @Test
  public void testDrill_1314() throws Exception {
    compareParquetReadersColumnar("l_partkey ", "dfs.`tmp/drill_1314.parquet`");
  }

  @Ignore("Binary file too large for version control")
  @Test
  public void testDrill_1314_all_columns() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`tmp/drill_1314.parquet`");
    compareParquetReadersColumnar(
        "l_orderkey,l_partkey,l_suppkey,l_linenumber, l_quantity, l_extendedprice,l_discount,l_tax",
        "dfs.`tmp/drill_1314.parquet`");
  }

  @Ignore("Test file not available")
  @Test
  public void testParquetRead_checkShortNullLists() throws Exception {
    compareParquetReadersColumnar("*", "dfs.`tmp/short_null_lists.parquet`");
  }

  @Ignore("Test file not available")
  @Test
  public void testParquetRead_checkStartWithNull() throws Exception {
    compareParquetReadersColumnar("*", "dfs.`tmp/start_with_null.parquet`");
  }

  @Ignore("Binary file too large for version control")
  @Test
  public void testParquetReadWebReturns() throws Exception {
    compareParquetReadersColumnar("wr_returning_customer_sk", "dfs.`tmp/web_returns`");
  }

  @Test
  public void testWriteDecimal() throws Exception {
    String outputTable = "decimal_test";

    try {
      test("use dfs.tmp; " +
        "create table %s as select " +
        "cast('1.2' as decimal(38, 2)) col1, cast('1.2' as decimal(28, 2)) col2 " +
        "from cp.`employee.json` limit 1", outputTable);

      BigDecimal result = new BigDecimal("1.20");

      testBuilder()
          .unOrdered()
          .sqlQuery("select col1, col2 from %s ", outputTable)
          .baselineColumns("col1", "col2")
          .baselineValues(result, result)
          .go();
    } finally {
      deleteTableIfExists(outputTable);
    }
  }

  @Test // DRILL-2341
  @Category(UnlikelyTest.class)
  public void tableSchemaWhenSelectFieldsInDef_SelectFieldsInView() throws Exception {
    final String newTblName = "testTableOutputSchema";

    try {
      test("CREATE TABLE dfs.tmp.%s(id, name, bday) AS SELECT " +
        "cast(`employee_id` as integer), " +
        "cast(`full_name` as varchar(100)), " +
        "cast(`birth_date` as date) " +
        "FROM cp.`employee.json` ORDER BY `employee_id` LIMIT 1", newTblName);

      testBuilder()
          .unOrdered()
          .sqlQuery("SELECT * FROM dfs.tmp.`%s`", newTblName)
          .baselineColumns("id", "name", "bday")
          .baselineValues(1, "Sheri Nowmer", LocalDate.parse("1961-08-26"))
          .go();
    } finally {
      deleteTableIfExists(newTblName);
    }
  }

  /*
 * Method tests CTAS with interval data type. We also verify reading back the data to ensure we
 * have written the correct type. For every CTAS operation we use both the readers to verify results.
 */
  @Test
  public void testCTASWithIntervalTypes() throws Exception {
    test("use dfs.tmp");

    String tableName = "drill_1980_t1";
    // test required interval day type
    test("create table %s as " +
        "select " +
        "interval '10 20:30:40.123' day to second col1, " +
        "interval '-1000000000 20:12:23.999' day(10) to second col2 " +
        "from cp.`employee.json` limit 2", tableName);

    Period row1Col1 = new Period(0, 0, 0, 10, 0, 0, 0, 73840123);
    Period row1Col2 = new Period(0, 0, 0, -1000000000, 0, 0, 0, -72743999);
    testParquetReaderHelper(tableName, row1Col1, row1Col2, row1Col1, row1Col2);

    tableName = "drill_1980_2";

    // test required interval year type
    test("create table %s as " +
        "select " +
        "interval '10-2' year to month col1, " +
        "interval '-100-8' year(3) to month col2 " +
        "from cp.`employee.json` limit 2", tableName);

    row1Col1 = new Period(0, 122, 0, 0, 0, 0, 0, 0);
    row1Col2 = new Period(0, -1208, 0, 0, 0, 0, 0, 0);

    testParquetReaderHelper(tableName, row1Col1, row1Col2, row1Col1, row1Col2);
    // test nullable interval year type
    tableName = "drill_1980_t3";
    test("create table %s as " +
        "select " +
        "cast (intervalyear_col as interval year) col1," +
        "cast(intervalyear_col as interval year) + interval '2' year col2 " +
        "from cp.`parquet/alltypes.json` where tinyint_col = 1 or tinyint_col = 2", tableName);

    row1Col1 = new Period(0, 12, 0, 0, 0, 0, 0, 0);
    row1Col2 = new Period(0, 36, 0, 0, 0, 0, 0, 0);
    Period row2Col1 = new Period(0, 24, 0, 0, 0, 0, 0, 0);
    Period row2Col2 = new Period(0, 48, 0, 0, 0, 0, 0, 0);

    testParquetReaderHelper(tableName, row1Col1, row1Col2, row2Col1, row2Col2);

    // test nullable interval day type
    tableName = "drill_1980_t4";
    test("create table %s as " +
        "select " +
        "cast(intervalday_col as interval day) col1, " +
        "cast(intervalday_col as interval day) + interval '1' day col2 " +
        "from cp.`parquet/alltypes.json` where tinyint_col = 1 or tinyint_col = 2", tableName);

    row1Col1 = new Period(0, 0, 0, 1, 0, 0, 0, 0);
    row1Col2 = new Period(0, 0, 0, 2, 0, 0, 0, 0);
    row2Col1 = new Period(0, 0, 0, 2, 0, 0, 0, 0);
    row2Col2 = new Period(0, 0, 0, 3, 0, 0, 0, 0);

    testParquetReaderHelper(tableName, row1Col1, row1Col2, row2Col1, row2Col2);
  }

  private void testParquetReaderHelper(String tableName, Period row1Col1, Period row1Col2,
                                       Period row2Col1, Period row2Col2) throws Exception {

    final String switchReader = "alter session set `store.parquet.use_new_reader` = %s; ";
    final String enableVectorizedReader = String.format(switchReader, true);
    final String disableVectorizedReader = String.format(switchReader, false);
    String query = String.format("select * from %s", tableName);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .optionSettingQueriesForTestQuery(enableVectorizedReader)
        .baselineColumns("col1", "col2")
        .baselineValues(row1Col1, row1Col2)
        .baselineValues(row2Col1, row2Col2)
        .go();

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .optionSettingQueriesForTestQuery(disableVectorizedReader)
        .baselineColumns("col1", "col2")
        .baselineValues(row1Col1, row1Col2)
        .baselineValues(row2Col1, row2Col2)
        .go();
  }

  private static void deleteTableIfExists(String tableName) {
    try {
      Path path = new Path(dirTestWatcher.getDfsTestTmpDir().getAbsolutePath(), tableName);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
    } catch (Exception e) {
      // ignore exceptions.
    }
  }

  public void runTestAndValidate(String selection, String validationSelection, String inputTable, String outputFile) throws Exception {
    try {
      deleteTableIfExists(outputFile);

      final String query = String.format("SELECT %s FROM %s", selection, inputTable);

      test("use dfs.tmp");
      test("CREATE TABLE %s AS %s", outputFile, query);
      testBuilder()
          .unOrdered()
          .sqlQuery(query)
          .sqlBaselineQuery("SELECT %s FROM %s", validationSelection, outputFile)
          .go();

      Configuration hadoopConf = new Configuration();
      hadoopConf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);

      @SuppressWarnings("resource")
      Path output = new Path(dirTestWatcher.getDfsTestTmpDir().getAbsolutePath(), outputFile);
      FileSystem fs = output.getFileSystem(hadoopConf);
      for (FileStatus file : fs.listStatus(output)) {
        ParquetMetadata footer = ParquetFileReader.readFooter(hadoopConf, file, SKIP_ROW_GROUPS);
        String version = footer.getFileMetaData().getKeyValueMetaData().get(DRILL_VERSION_PROPERTY);
        assertEquals(DrillVersionInfo.getVersion(), version);
      }
    } finally {
      deleteTableIfExists(outputFile);
    }
  }

  /*
    Impala encodes timestamp values as int96 fields. Test the reading of an int96 field with two converters:
    the first one converts parquet INT96 into drill VARBINARY and the second one (works while
    store.parquet.reader.int96_as_timestamp option is enabled) converts parquet INT96 into drill TIMESTAMP.
   */
  @Test
  public void testImpalaParquetInt96() throws Exception {
    compareParquetReadersColumnar("field_impala_ts", "cp.`parquet/int96_impala_1.parquet`");
    try {
      alterSession(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP, true);
      compareParquetReadersColumnar("field_impala_ts", "cp.`parquet/int96_impala_1.parquet`");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP);
    }
  }

  /*
  Test the reading of a binary field as drill varbinary where data is in dictionary _and_ non-dictionary encoded pages
   */
  @Test
  public void testImpalaParquetBinaryAsVarBinary_DictChange() throws Exception {
    compareParquetReadersColumnar("field_impala_ts", "cp.`parquet/int96_dict_change.parquet`");
  }

  /*
  Test the reading of a binary field as drill timestamp where data is in dictionary _and_ non-dictionary encoded pages
   */
  @Test
  public void testImpalaParquetBinaryAsTimeStamp_DictChange() throws Exception {
    try {
      testBuilder()
          .sqlQuery("select min(int96_ts) date_value from dfs.`parquet/int96_dict_change`")
          .optionSettingQueriesForTestQuery(
              "alter session set `%s` = true", ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP)
          .ordered()
          .baselineColumns("date_value")
          .baselineValues(convertToLocalDateTime("1970-01-01 00:00:01.000"))
          .build().run();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP);
    }
  }

  @Test
  public void testSparkParquetBinaryAsTimeStamp_DictChange() throws Exception {
    try {
      testBuilder()
          .sqlQuery("select distinct run_date from cp.`parquet/spark-generated-int96-timestamp.snappy.parquet`")
          .optionSettingQueriesForTestQuery(
               "alter session set `%s` = true", ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP)
          .ordered()
          .baselineColumns("run_date")
          .baselineValues(convertToLocalDateTime("2017-12-06 16:38:43.988"))
          .build().run();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP);
    }
  }

  /*
     Test the conversion from int96 to impala timestamp
   */
  @Test
  public void testTimestampImpalaConvertFrom() throws Exception {
    compareParquetReadersColumnar("convert_from(field_impala_ts, 'TIMESTAMP_IMPALA')", "cp.`parquet/int96_impala_1.parquet`");
  }

  /*
     Test reading parquet Int96 as TimeStamp and comparing obtained values with the
     old results (reading the same values as VarBinary and convert_fromTIMESTAMP_IMPALA function using)
   */
  @Test
  public void testImpalaParquetTimestampInt96AsTimeStamp() throws Exception {
    try {
      alterSession(ExecConstants.PARQUET_NEW_RECORD_READER, false);
      compareParquetInt96Converters("field_impala_ts", "cp.`parquet/int96_impala_1.parquet`");
      alterSession(ExecConstants.PARQUET_NEW_RECORD_READER, true);
      compareParquetInt96Converters("field_impala_ts", "cp.`parquet/int96_impala_1.parquet`");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_NEW_RECORD_READER);
    }
  }

  /*
    Test a file with partitions and an int96 column. (Data generated using Hive)
   */
  @Test
  public void testImpalaParquetInt96Partitioned() throws Exception {
    compareParquetReadersColumnar("timestamp_field", "cp.`parquet/part1/hive_all_types.parquet`");
  }

  /*
  Test the conversion from int96 to impala timestamp with hive data including nulls. Validate against old reader
  */
  @Test
  public void testHiveParquetTimestampAsInt96_compare() throws Exception {
    compareParquetReadersColumnar("convert_from(timestamp_field, 'TIMESTAMP_IMPALA')",
        "cp.`parquet/part1/hive_all_types.parquet`");
  }

  /*
  Test the conversion from int96 to impala timestamp with hive data including nulls. Validate against expected values
  */
  @Test
  public void testHiveParquetTimestampAsInt96_basic() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT convert_from(timestamp_field, 'TIMESTAMP_IMPALA')  as timestamp_field "
             + "from cp.`parquet/part1/hive_all_types.parquet` ")
        .baselineColumns("timestamp_field")
        .baselineValues(convertToLocalDateTime("2013-07-06 00:01:00"))
        .baselineValues((Object)null)
        .go();
  }

  @Test
  @Ignore
  public void testSchemaChange() throws Exception {
    File dir = new File("target/" + this.getClass());
    if ((!dir.exists() && !dir.mkdirs()) || (dir.exists() && !dir.isDirectory())) {
      throw new RuntimeException("can't create dir " + dir);
  }
    File input1 = new File(dir, "1.json");
    File input2 = new File(dir, "2.json");
    try (FileWriter fw = new FileWriter(input1)) {
      fw.append("{\"a\":\"foo\"}\n");
    }
    try (FileWriter fw = new FileWriter(input2)) {
      fw.append("{\"b\":\"foo\"}\n");
    }
    test("select * from " + "dfs.`" + dir.getAbsolutePath() + "`");
    runTestAndValidate("*", "*", "dfs.`" + dir.getAbsolutePath() + "`", "schema_change_parquet");
  }


/*
  The following test boundary conditions for null values occurring on page boundaries. All files have at least one dictionary
  encoded page for all columns
  */
  @Test
  public void testAllNulls() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.`parquet/all_nulls.parquet`");
  }

  @Test
  public void testNoNulls() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.`parquet/no_nulls.parquet`");
  }

  @Test
  public void testFirstPageAllNulls() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.`parquet/first_page_all_nulls.parquet`");
  }
  @Test
  public void testLastPageAllNulls() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.`parquet/first_page_all_nulls.parquet`");
  }
  @Test
  public void testFirstPageOneNull() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.`parquet/first_page_one_null.parquet`");
  }
  @Test
  public void testLastPageOneNull() throws Exception {
    compareParquetReadersColumnar(
        "c_varchar, c_integer, c_bigint, c_float, c_double, c_date, c_time, c_timestamp, c_boolean",
        "cp.`parquet/last_page_one_null.parquet`");
  }

  private void compareParquetInt96Converters(String selection, String table) throws Exception {
    try {
      testBuilder()
          .ordered()
          .sqlQuery("select `%1$s` from %2$s order by `%1$s`", selection, table)
          .optionSettingQueriesForTestQuery(
              "alter session set `%s` = true", ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP)
          .sqlBaselineQuery("select convert_from(`%1$s`, 'TIMESTAMP_IMPALA') as `%1$s` from %2$s order by `%1$s`",
              selection, table)
          .optionSettingQueriesForBaseline(
              "alter session set `%s` = false", ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP)
          .build()
          .run();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP);
    }
  }

  @Ignore ("Used to test decompression in AsyncPageReader. Takes too long.")
  @Test
  public void testTPCHReadWriteRunRepeated() throws Exception {
    for (int i = 1; i <= repeat; i++) {
      testTPCHReadWriteGzip();
      testTPCHReadWriteSnappy();
    }
  }

  @Test
  public void testTPCHReadWriteGzip() throws Exception {
    try {
      alterSession(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE, "gzip");
      String inputTable = "cp.`tpch/supplier.parquet`";
        runTestAndValidate("*", "*", inputTable, "suppkey_parquet_dict_gzip");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE);
    }
  }

  @Test
  public void testTPCHReadWriteSnappy() throws Exception {
    try {
      alterSession(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE, "snappy");
      String inputTable = "cp.`supplier_snappy.parquet`";
      runTestAndValidate("*", "*", inputTable, "suppkey_parquet_dict_snappy");
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE);
    }
  }

  @Test // DRILL-5097
  @Category(UnlikelyTest.class)
  public void testInt96TimeStampValueWidth() throws Exception {
    try {
      testBuilder()
          .unOrdered()
          .sqlQuery("select c, d from cp.`parquet/data.snappy.parquet` " +
              "where `a` is not null and `c` is not null and `d` is not null")
          .optionSettingQueriesForTestQuery(
              "alter session set `%s` = true", ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP)
          .baselineColumns("c", "d")
          .baselineValues(LocalDate.parse("2012-12-15"),
                  convertToLocalDateTime("2016-04-24 20:06:28"))
          .baselineValues(LocalDate.parse("2011-07-09"),
                  convertToLocalDateTime("2015-04-15 22:35:49"))
          .build()
          .run();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_READER_INT96_AS_TIMESTAMP);
    }
  }

  @Test
  public void testWriteDecimalIntBigIntFixedLen() throws Exception {
    String tableName = "decimalIntBigIntFixedLen";
    try {
      alterSession(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS, FIXED_LEN_BYTE_ARRAY.name());
      test(
          "create table dfs.tmp.%s as\n" +
              "select cast('123456.789' as decimal(9, 3)) as decInt,\n" +
                     "cast('123456.789123456789' as decimal(18, 12)) as decBigInt,\n" +
                     "cast('123456.789123456789' as decimal(19, 12)) as fixedLen", tableName);
      checkTableTypes(tableName,
          ImmutableList.of(
              Pair.of("decInt", INT32),
              Pair.of("decBigInt", INT64),
              Pair.of("fixedLen", FIXED_LEN_BYTE_ARRAY)),
          true);
      testBuilder()
          .sqlQuery("select * from dfs.tmp.%s", tableName)
          .unOrdered()
          .baselineColumns("decInt", "decBigInt", "fixedLen")
          .baselineValues(new BigDecimal("123456.789"),
              new BigDecimal("123456.789123456789"),
              new BigDecimal("123456.789123456789"))
          .go();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS);
      resetSessionOption(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS);
      test("drop table if exists dfs.tmp.%s", tableName);
    }
  }

  @Test
  public void testWriteDecimalIntBigIntBinary() throws Exception {
    String tableName = "decimalIntBigIntBinary";
    try {
      alterSession(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS, true);
      alterSession(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS, BINARY.name());
      test(
          "create table dfs.tmp.%s as\n" +
              "select cast('123456.789' as decimal(9, 3)) as decInt,\n" +
                     "cast('123456.789123456789' as decimal(18, 12)) as decBigInt,\n" +
                     "cast('123456.789123456789' as decimal(19, 12)) as binCol", tableName);
      checkTableTypes(tableName,
          ImmutableList.of(
              Pair.of("decInt", INT32),
              Pair.of("decBigInt", INT64),
              Pair.of("binCol", BINARY)),
          true);
      testBuilder()
          .sqlQuery("select * from dfs.tmp.%s", tableName)
          .unOrdered()
          .baselineColumns("decInt", "decBigInt", "binCol")
          .baselineValues(new BigDecimal("123456.789"),
            new BigDecimal("123456.789123456789"),
            new BigDecimal("123456.789123456789"))
          .go();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS);
      resetSessionOption(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS);
      test("drop table if exists dfs.tmp.%s", tableName);
    }
  }

  @Test
  public void testWriteDecimalFixedLenOnly() throws Exception {
    String tableName = "decimalFixedLenOnly";
    try {
      alterSession(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS, false);
      alterSession(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS, FIXED_LEN_BYTE_ARRAY.name());
      test(
          "create table dfs.tmp.%s as\n" +
              "select cast('123456.789' as decimal(9, 3)) as decInt,\n" +
                     "cast('123456.789123456789' as decimal(18, 12)) as decBigInt,\n" +
                     "cast('123456.789123456789' as decimal(19, 12)) as fixedLen", tableName);
      checkTableTypes(tableName,
          ImmutableList.of(
              Pair.of("decInt", FIXED_LEN_BYTE_ARRAY),
              Pair.of("decBigInt", FIXED_LEN_BYTE_ARRAY),
              Pair.of("fixedLen", FIXED_LEN_BYTE_ARRAY)),
          true);
      testBuilder()
          .sqlQuery("select * from dfs.tmp.%s", tableName)
          .unOrdered()
          .baselineColumns("decInt", "decBigInt", "fixedLen")
          .baselineValues(new BigDecimal("123456.789"),
            new BigDecimal("123456.789123456789"),
            new BigDecimal("123456.789123456789"))
          .go();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS);
      resetSessionOption(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS);
      test("drop table if exists dfs.tmp.%s", tableName);
    }
  }

  @Test
  public void testWriteDecimalBinaryOnly() throws Exception {
    String tableName = "decimalBinaryOnly";
    try {
      alterSession(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS, false);
      alterSession(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS, BINARY.name());
      test(
        "create table dfs.tmp.%s as\n" +
            "select cast('123456.789' as decimal(9, 3)) as decInt,\n" +
                   "cast('123456.789123456789' as decimal(18, 12)) as decBigInt,\n" +
                   "cast('123456.789123456789' as decimal(19, 12)) as binCol", tableName);
      checkTableTypes(tableName,
        ImmutableList.of(
          Pair.of("decInt", BINARY),
          Pair.of("decBigInt", BINARY),
          Pair.of("binCol", BINARY)),
        true);
      testBuilder()
          .sqlQuery("select * from dfs.tmp.%s", tableName)
          .unOrdered()
          .baselineColumns("decInt", "decBigInt", "binCol")
          .baselineValues(new BigDecimal("123456.789"),
            new BigDecimal("123456.789123456789"),
            new BigDecimal("123456.789123456789"))
          .go();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS);
      resetSessionOption(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS);
      test("drop table if exists dfs.tmp.%s", tableName);
    }
  }

  @Test
  public void testWriteDecimalIntBigIntRepeated() throws Exception {
    String tableName = "decimalIntBigIntRepeated";

    JsonStringArrayList<BigDecimal> ints = new JsonStringArrayList<>();
    ints.add(new BigDecimal("999999.999"));
    ints.add(new BigDecimal("-999999.999"));
    ints.add(new BigDecimal("0.000"));

    JsonStringArrayList<BigDecimal> longs = new JsonStringArrayList<>();
    longs.add(new BigDecimal("999999999.999999999"));
    longs.add(new BigDecimal("-999999999.999999999"));
    longs.add(new BigDecimal("0.000000000"));

    JsonStringArrayList<BigDecimal> fixedLen = new JsonStringArrayList<>();
    fixedLen.add(new BigDecimal("999999999999.999999"));
    fixedLen.add(new BigDecimal("-999999999999.999999"));
    fixedLen.add(new BigDecimal("0.000000"));

    try {
      alterSession(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS, true);
      alterSession(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS, FIXED_LEN_BYTE_ARRAY.name());
      test(
        "create table dfs.tmp.%s as\n" +
            "select * from cp.`parquet/repeatedIntLondFixedLenBinaryDecimal.parquet`", tableName);
      checkTableTypes(tableName,
          ImmutableList.of(
              Pair.of("decimal_int32", INT32),
              Pair.of("decimal_int64", INT64),
              Pair.of("decimal_fixedLen", INT64),
              Pair.of("decimal_binary", INT64)),
          true);
      testBuilder()
          .sqlQuery("select * from dfs.tmp.%s", tableName)
          .unOrdered()
          .baselineColumns("decimal_int32", "decimal_int64", "decimal_fixedLen", "decimal_binary")
          .baselineValues(ints, longs, fixedLen, fixedLen)
          .go();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS);
      resetSessionOption(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS);
      test("drop table if exists dfs.tmp.%s", tableName);
    }
  }

  @Test
  public void testWriteDecimalFixedLenRepeated() throws Exception {
    String tableName = "decimalFixedLenRepeated";

    JsonStringArrayList<BigDecimal> ints = new JsonStringArrayList<>();
    ints.add(new BigDecimal("999999.999"));
    ints.add(new BigDecimal("-999999.999"));
    ints.add(new BigDecimal("0.000"));

    JsonStringArrayList<BigDecimal> longs = new JsonStringArrayList<>();
    longs.add(new BigDecimal("999999999.999999999"));
    longs.add(new BigDecimal("-999999999.999999999"));
    longs.add(new BigDecimal("0.000000000"));

    JsonStringArrayList<BigDecimal> fixedLen = new JsonStringArrayList<>();
    fixedLen.add(new BigDecimal("999999999999.999999"));
    fixedLen.add(new BigDecimal("-999999999999.999999"));
    fixedLen.add(new BigDecimal("0.000000"));

    try {
      alterSession(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS, false);
      alterSession(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS, FIXED_LEN_BYTE_ARRAY.name());
      test(
          "create table dfs.tmp.%s as\n" +
              "select * from cp.`parquet/repeatedIntLondFixedLenBinaryDecimal.parquet`", tableName);
      checkTableTypes(tableName,
          ImmutableList.of(
              Pair.of("decimal_int32", FIXED_LEN_BYTE_ARRAY),
              Pair.of("decimal_int64", FIXED_LEN_BYTE_ARRAY),
              Pair.of("decimal_fixedLen", FIXED_LEN_BYTE_ARRAY),
              Pair.of("decimal_binary", FIXED_LEN_BYTE_ARRAY)),
          true);
      testBuilder()
          .sqlQuery("select * from dfs.tmp.%s", tableName)
          .unOrdered()
          .baselineColumns("decimal_int32", "decimal_int64", "decimal_fixedLen", "decimal_binary")
          .baselineValues(ints, longs, fixedLen, fixedLen)
          .go();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS);
      resetSessionOption(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS);
      test("drop table if exists dfs.tmp.%s", tableName);
    }
  }

  @Test
  public void testWriteDecimalBinaryRepeated() throws Exception {
    String tableName = "decimalBinaryRepeated";

    JsonStringArrayList<BigDecimal> ints = new JsonStringArrayList<>();
    ints.add(new BigDecimal("999999.999"));
    ints.add(new BigDecimal("-999999.999"));
    ints.add(new BigDecimal("0.000"));

    JsonStringArrayList<BigDecimal> longs = new JsonStringArrayList<>();
    longs.add(new BigDecimal("999999999.999999999"));
    longs.add(new BigDecimal("-999999999.999999999"));
    longs.add(new BigDecimal("0.000000000"));

    JsonStringArrayList<BigDecimal> fixedLen = new JsonStringArrayList<>();
    fixedLen.add(new BigDecimal("999999999999.999999"));
    fixedLen.add(new BigDecimal("-999999999999.999999"));
    fixedLen.add(new BigDecimal("0.000000"));
    try {
      alterSession(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS, false);
      alterSession(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS, BINARY.name());
      test(
        "create table dfs.tmp.%s as\n" +
            "select * from cp.`parquet/repeatedIntLondFixedLenBinaryDecimal.parquet`", tableName);
      checkTableTypes(tableName,
          ImmutableList.of(
              Pair.of("decimal_int32", BINARY),
              Pair.of("decimal_int64", BINARY),
              Pair.of("decimal_fixedLen", BINARY),
              Pair.of("decimal_binary", BINARY)),
          true);
      testBuilder()
          .sqlQuery("select * from dfs.tmp.%s", tableName)
          .unOrdered()
          .baselineColumns("decimal_int32", "decimal_int64", "decimal_fixedLen", "decimal_binary")
          .baselineValues(ints, longs, fixedLen, fixedLen)
          .go();
    } finally {
      resetSessionOption(ExecConstants.PARQUET_WRITER_USE_PRIMITIVE_TYPES_FOR_DECIMALS);
      resetSessionOption(ExecConstants.PARQUET_WRITER_LOGICAL_TYPE_FOR_DECIMALS);
      test("drop table if exists dfs.tmp.%s", tableName);
    }
  }

  /**
   * Checks that specified parquet table contains specified columns with specified types.
   *
   * @param tableName      name of the table that should be checked.
   * @param columnsToCheck pair of column name and column type that should be checked in the table.
   * @param isDecimalType  is should be specified columns annotated ad DECIMAL.
   * @throws IOException If table file was not found.
   */
  private void checkTableTypes(String tableName,
      List<Pair<String, PrimitiveType.PrimitiveTypeName>> columnsToCheck,
      boolean isDecimalType) throws IOException {
    MessageType schema = ParquetFileReader.readFooter(
        new Configuration(),
        new Path(Paths.get(dirTestWatcher.getDfsTestTmpDir().getPath(), tableName, "0_0_0.parquet").toUri().getPath()),
        NO_FILTER).getFileMetaData().getSchema();

    for (Pair<String, PrimitiveType.PrimitiveTypeName> nameType : columnsToCheck) {
      assertEquals(
          String.format("Table %s does not contain column %s with type %s",
              tableName, nameType.getKey(), nameType.getValue()),
          nameType.getValue(),
          schema.getType(nameType.getKey()).asPrimitiveType().getPrimitiveTypeName());

      assertEquals(
        String.format("Table %s %s column %s with DECIMAL type", tableName,
            isDecimalType ? "does not contain" : "contains unexpected", nameType.getKey()),
        isDecimalType, schema.getType(nameType.getKey()).getOriginalType() == OriginalType.DECIMAL);
    }
  }
}
