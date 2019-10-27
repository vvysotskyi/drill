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
package org.apache.drill.exec.metastore.analyze;

import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.metastore.components.tables.BasicTablesRequests;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.TableInfo;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public interface AnalyzeInfoProvider {

  List<SchemaPath> getSegmentColumns(DrillTable table, OptionManager options) throws IOException;
  List<SqlIdentifier> getProjectionFields(MetadataType metadataLevel, OptionManager options);

  MetadataInfoCollector getMetadataInfoCollector(BasicTablesRequests basicRequests, TableInfo tableInfo,
      FormatSelection selection, PlannerSettings settings, Supplier<TableScan> scanRel,
      List<SchemaPath> interestingColumns, MetadataType metadataLevel, int segmentColumnsCount) throws IOException;

  static AnalyzeInfoProvider getAnalyzeInfoProvider(TableType tableType) {
    switch (tableType) {
      case PARQUET:
        return AnalyzeFileInfoProvider.INSTANCE;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported table type [%s]", tableType));
    }
  }
}
