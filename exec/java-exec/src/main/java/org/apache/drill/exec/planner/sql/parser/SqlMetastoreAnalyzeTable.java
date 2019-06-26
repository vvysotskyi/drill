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
package org.apache.drill.exec.planner.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.MetastoreAnalyzeTableHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import java.util.List;

public class SqlMetastoreAnalyzeTable extends DrillSqlCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("METASTORE_ANALYZE_TABLE", SqlKind.OTHER) {
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlMetastoreAnalyzeTable.createCall() has to get 3 operands.");
      return new SqlMetastoreAnalyzeTable(pos, (SqlIdentifier) operands[0], (SqlNodeList) operands[1], operands[2]
      );
    }
  };

  private final SqlIdentifier tblName;
  private final SqlNodeList fieldList;
  private final SqlNode level;

  public SqlMetastoreAnalyzeTable(SqlParserPos pos, SqlIdentifier tblName, SqlNodeList fieldList, SqlNode level) {
    super(pos);
    this.tblName = tblName;
    this.fieldList = fieldList;
    this.level = level;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    final List<SqlNode> operands = Lists.newArrayListWithCapacity(3);
    operands.add(tblName);
    operands.add(fieldList);
    operands.add(level);
    return operands;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ANALYZE");
    writer.keyword("TABLE");
    tblName.unparse(writer, leftPrec, rightPrec);
    if (fieldList != null && fieldList.size() > 0) {
      writer.keyword("COLUMNS");
      writer.keyword("(");
      fieldList.get(0).unparse(writer, leftPrec, rightPrec);
      for (int i = 1; i < fieldList.size(); i++) {
        writer.keyword(",");
        fieldList.get(i).unparse(writer, leftPrec, rightPrec);
      }
      writer.keyword(")");
    }
    writer.keyword("REFRESH METADATA");
    if (level != null) {
      level.unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    return new MetastoreAnalyzeTableHandler(config, textPlan);
  }

  @Override
  public AbstractSqlHandler getSqlHandler(SqlHandlerConfig config) {
    return getSqlHandler(config, null);
  }

  public List<String> getSchemaPath() {
    if (tblName.isSimple()) {
      return ImmutableList.of();
    }

    return tblName.names.subList(0, tblName.names.size() - 1);
  }

  public SqlIdentifier getTableIdentifier() {
    return tblName;
  }

  public String getName() {
    return Util.last(tblName.names);
  }

  public List<String> getFieldNames() {
    if (fieldList == null) {
      return ImmutableList.of();
    }

    List<String> columnNames = Lists.newArrayList();
    for (SqlNode node : fieldList.getList()) {
      columnNames.add(node.toString());
    }
    return columnNames;
  }

  public SqlNodeList getFieldList() {
    return fieldList;
  }

  public SqlNode getLevel() {
    return level;
  }
}
