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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.metastore.metadata.MetadataType;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link AnalyzeInfoProvider} for parquet tables.
 */
public class AnalyzeParquetInfoProvider extends AnalyzeFileInfoProvider {
  public static final AnalyzeInfoProvider INSTANCE = new AnalyzeParquetInfoProvider();

  @Override
  public List<SqlIdentifier> getProjectionFields(MetadataType metadataLevel, OptionManager options) {
    List<SqlIdentifier> columnList = new ArrayList<>(super.getProjectionFields(metadataLevel, options));
    if (metadataLevel.compareTo(MetadataType.ROW_GROUP) >= 0) {
      columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_ROW_GROUP_INDEX_COLUMN_LABEL), SqlParserPos.ZERO));
      columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_ROW_GROUP_START_COLUMN_LABEL), SqlParserPos.ZERO));
      columnList.add(new SqlIdentifier(options.getString(ExecConstants.IMPLICIT_ROW_GROUP_LEHGTH_COLUMN_LABEL), SqlParserPos.ZERO));
    }
    return columnList;
  }
}
