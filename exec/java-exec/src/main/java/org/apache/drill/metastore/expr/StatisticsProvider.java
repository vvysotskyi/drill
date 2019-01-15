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
package org.apache.drill.metastore.expr;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedFieldExpr;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.FuncHolder;
import org.apache.drill.common.expression.fn.FunctionReplacementUtils;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.apache.drill.metastore.ColumnStatistic;
import org.apache.drill.metastore.ColumnStatisticImpl;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.StatisticsKind;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Comparator;
import java.util.Map;

import static org.apache.drill.exec.expr.stat.RangeExprEvaluator.CAST_FUNC;
import static org.apache.drill.metastore.expr.ComparisonPredicate.getMaxValue;
import static org.apache.drill.metastore.expr.ComparisonPredicate.getMinValue;
import static org.apache.drill.metastore.expr.IsPredicate.isNullOrEmpty;

public class StatisticsProvider<T extends Comparable<T>> extends AbstractExprVisitor<ColumnStatistic, Void, RuntimeException> {

  public static final Comparator<byte[]> UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR =
      Comparator.nullsFirst((b1, b2) -> PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(Binary.fromReusedByteArray(b1), Binary.fromReusedByteArray(b2)));

  private static final PrimitiveType UINT32_TYPE = org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
      .as(OriginalType.UINT_32)
      .named("unsigned_type");

  public static final Comparator<Integer> UINT32_COMPARATOR =
      Comparator.nullsFirst((i1, i2) -> Statistics.getBuilderForReading(UINT32_TYPE).build().comparator().compare(i1, i2));

  private static final PrimitiveType UINT64_TYPE = org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
      .as(OriginalType.UINT_64)
      .named("unsigned_type");

  public static final Comparator<Long> UINT64_COMPARATOR =
      Comparator.nullsFirst((i1, i2) -> Statistics.getBuilderForReading(UINT64_TYPE).build().comparator().compare(i1, i2));

  public static Comparator<byte[]> BINARY_AS_SIGNED_INTEGER_COMPARATOR = Comparator.nullsFirst((b1, b2) ->
      org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(OriginalType.DECIMAL)
            .length(1)
            .precision(1)
            .scale(0)
            .named("decimal_type")
          .comparator()
        .compare(Binary.fromReusedByteArray(b1), Binary.fromReusedByteArray(b2)));

  private final Map<SchemaPath, ColumnStatistic> columnStatMap;
  private final long rowCount;

  public StatisticsProvider(final Map<SchemaPath, ColumnStatistic> columnStatMap, long rowCount) {
    this.columnStatMap = columnStatMap;
    this.rowCount = rowCount;
  }

  public long getRowCount() {
    return this.rowCount;
  }

  @Override
  public ColumnStatisticImpl visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    // do nothing for the unknown expression
    return null;
  }

  @Override
  public ColumnStatistic visitTypedFieldExpr(TypedFieldExpr typedFieldExpr, Void value) throws RuntimeException {
    final ColumnStatistic columnStatistic = columnStatMap.get(typedFieldExpr.getPath().getUnIndexed());
    if (columnStatistic != null) {
      return columnStatistic;
    } else if (typedFieldExpr.getMajorType().equals(Types.OPTIONAL_INT)) {
      // field does not exist.
      MinMaxStatistics<Integer> statistics = new MinMaxStatistics<>(null, null, Integer::compareTo);
      statistics.setNullsCount(rowCount); // all values are nulls
      return statistics;
    }
    return null;
  }

  @Override
  public ColumnStatistic<Integer> visitIntConstant(ValueExpressions.IntExpression expr, Void value) throws RuntimeException {
    int exprValue = expr.getInt();
    return new MinMaxStatistics<>(exprValue, exprValue, Integer::compareTo);
  }

  @Override
  public ColumnStatistic<Boolean> visitBooleanConstant(ValueExpressions.BooleanExpression expr, Void value) throws RuntimeException {
    boolean exprValue = expr.getBoolean();
    return new MinMaxStatistics<>(exprValue, exprValue, Boolean::compareTo);
  }

  @Override
  public ColumnStatistic<Long> visitLongConstant(ValueExpressions.LongExpression expr, Void value) throws RuntimeException {
    long exprValue = expr.getLong();
    return new MinMaxStatistics<>(exprValue, exprValue, Long::compareTo);
  }

  @Override
  public ColumnStatistic<Float> visitFloatConstant(ValueExpressions.FloatExpression expr, Void value) throws RuntimeException {
    float exprValue = expr.getFloat();
    return new MinMaxStatistics<>(exprValue, exprValue, Float::compareTo);
  }

  @Override
  public ColumnStatistic<Double> visitDoubleConstant(ValueExpressions.DoubleExpression expr, Void value) throws RuntimeException {
    double exprValue = expr.getDouble();
    return new MinMaxStatistics<>(exprValue, exprValue, Double::compareTo);
  }

  @Override
  public ColumnStatistic<Long> visitDateConstant(ValueExpressions.DateExpression expr, Void value) throws RuntimeException {
    long exprValue = expr.getDate();
    return new MinMaxStatistics<>(exprValue, exprValue, Long::compareTo);
  }

  @Override
  public ColumnStatistic<Long> visitTimeStampConstant(ValueExpressions.TimeStampExpression tsExpr, Void value) throws RuntimeException {
    long exprValue = tsExpr.getTimeStamp();
    return new MinMaxStatistics<>(exprValue, exprValue, Long::compareTo);
  }

  @Override
  public ColumnStatistic<Integer> visitTimeConstant(ValueExpressions.TimeExpression timeExpr, Void value) throws RuntimeException {
    int exprValue = timeExpr.getTime();
    return new MinMaxStatistics<>(exprValue, exprValue, Integer::compareTo);
  }

  @Override
  public ColumnStatistic<byte[]> visitQuotedStringConstant(ValueExpressions.QuotedString quotedString, Void value) throws RuntimeException {
    byte[] binary = quotedString.getString().getBytes();
    return new MinMaxStatistics<>(binary, binary, UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR);
  }

  @Override
  public ColumnStatistic<byte[]> visitVarDecimalConstant(ValueExpressions.VarDecimalExpression decExpr, Void value) throws RuntimeException {
    byte[] bytes = decExpr.getBigDecimal().unscaledValue().toByteArray();
    return new MinMaxStatistics<>(
        bytes,
        bytes,
        BINARY_AS_SIGNED_INTEGER_COMPARATOR);
  }

  @Override
  public ColumnStatistic visitFunctionHolderExpression(FunctionHolderExpression holderExpr, Void value) throws RuntimeException {
    FuncHolder funcHolder = holderExpr.getHolder();

    if (! (funcHolder instanceof DrillSimpleFuncHolder)) {
      // Only Drill function is allowed.
      return null;
    }

    final String funcName = ((DrillSimpleFuncHolder) funcHolder).getRegisteredNames()[0];

    if (FunctionReplacementUtils.isCastFunction(funcName)) {
      ColumnStatistic<T> stat = holderExpr.args.get(0).accept(this, null);
      if (!isNullOrEmpty(stat)) {
        return evalCastFunc(holderExpr, stat);
      }
    }
    return null;
  }

  private ColumnStatistic<T> evalCastFunc(FunctionHolderExpression holderExpr, ColumnStatistic<T> input) {
    try {
      DrillSimpleFuncHolder funcHolder = (DrillSimpleFuncHolder) holderExpr.getHolder();

      DrillSimpleFunc interpreter = funcHolder.createInterpreter();

      final ValueHolder minHolder, maxHolder;

      TypeProtos.MinorType srcType = holderExpr.args.get(0).getMajorType().getMinorType();
      TypeProtos.MinorType destType = holderExpr.getMajorType().getMinorType();

      if (srcType.equals(destType)) {
        // same type cast ==> NoOp.
        return input;
      } else if (!CAST_FUNC.containsKey(srcType) || !CAST_FUNC.get(srcType).contains(destType)) {
        return null; // cast func between srcType and destType is NOT allowed.
      }

      switch (srcType) {
        case INT :
          minHolder = ValueHolderHelper.getIntHolder((Integer) getMinValue(input));
          maxHolder = ValueHolderHelper.getIntHolder((Integer) getMaxValue(input));
          break;
        case BIGINT:
          minHolder = ValueHolderHelper.getBigIntHolder((Long) getMinValue(input));
          maxHolder = ValueHolderHelper.getBigIntHolder((Long) getMaxValue(input));
          break;
        case FLOAT4:
          minHolder = ValueHolderHelper.getFloat4Holder((Float) getMinValue(input));
          maxHolder = ValueHolderHelper.getFloat4Holder((Float) getMaxValue(input));
          break;
        case FLOAT8:
          minHolder = ValueHolderHelper.getFloat8Holder((Double) getMinValue(input));
          maxHolder = ValueHolderHelper.getFloat8Holder((Double) getMaxValue(input));
          break;
        case DATE:
          minHolder = ValueHolderHelper.getDateHolder((Long) getMinValue(input));
          maxHolder = ValueHolderHelper.getDateHolder((Long) getMaxValue(input));
          break;
        default:
          return null;
      }

      final ValueHolder[] args1 = {minHolder};
      final ValueHolder[] args2 = {maxHolder};

      final ValueHolder minFuncHolder = InterpreterEvaluator.evaluateFunction(interpreter, args1, holderExpr.getName());
      final ValueHolder maxFuncHolder = InterpreterEvaluator.evaluateFunction(interpreter, args2, holderExpr.getName());

      MinMaxStatistics statistics;
      switch (destType) {
        case INT:
          statistics = new MinMaxStatistics<>(((IntHolder) minFuncHolder).value, ((IntHolder) maxFuncHolder).value, Integer::compareTo);
          break;
        case BIGINT:
          statistics = new MinMaxStatistics<>(((BigIntHolder) minFuncHolder).value, ((BigIntHolder) maxFuncHolder).value, Long::compareTo);
          break;
        case FLOAT4:
          statistics = new MinMaxStatistics<>(((Float4Holder) minFuncHolder).value, ((Float4Holder) maxFuncHolder).value, Float::compareTo);
          break;
        case FLOAT8:
          statistics = new MinMaxStatistics<>(((Float8Holder) minFuncHolder).value, ((Float8Holder) maxFuncHolder).value, Double::compareTo);
          break;
        case TIMESTAMP:
          statistics = new MinMaxStatistics<>(((TimeStampHolder) minFuncHolder).value, ((TimeStampHolder) maxFuncHolder).value, Long::compareTo);
          break;
        default:
          return null;
      }
      statistics.setNullsCount((long) input.getStatistic(ColumnStatisticsKind.NULLS_COUNT));
      return statistics;
    } catch (Exception e) {
      throw new DrillRuntimeException("Error in evaluating function of " + holderExpr.getName() );
    }
  }

  public static class MinMaxStatistics<V> implements ColumnStatistic<V> {
    private V minVal;
    private V maxVal;
    private long nullsCount;
    private Comparator<V> valueComparator;

    public MinMaxStatistics(V minVal, V maxVal, Comparator<V> valueComparator) {
      this.minVal = minVal;
      this.maxVal = maxVal;
      this.valueComparator = valueComparator;
    }

    @Override
    public Object getStatistic(StatisticsKind statisticsKind) {
      switch (statisticsKind.getName()) {
        case StatisticName.MIN_VALUE:
          return minVal;
        case StatisticName.MAX_VALUE:
          return maxVal;
        case StatisticName.NULLS_COUNT:
          return nullsCount;
//        case StatisticName.ROW_COUNT:
//          return 1;
        default:
          return null;
      }
    }

    @Override
    public boolean containsStatistic(StatisticsKind statisticsKind) {
      switch (statisticsKind.getName()) {
        case StatisticName.MIN_VALUE:
        case StatisticName.MAX_VALUE:
        case StatisticName.NULLS_COUNT:
          return true;
        default:
          return false;
      }
    }

    @Override
    public Comparator<V> getValueComparator() {
      return valueComparator;
    }

    public void setNullsCount(long nullsCount) {
      this.nullsCount = nullsCount;
    }
  }
}
