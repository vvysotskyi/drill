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

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.LogicalExpressionBase;
import org.apache.drill.common.expression.TypedFieldExpr;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.expr.stat.ParquetFilterPredicate;
import org.apache.drill.metastore.ColumnStatistic;
import org.apache.parquet.column.statistics.BooleanStatistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

public class IsPredicate<C extends Comparable<C>> extends LogicalExpressionBase
  implements FilterPredicate<C> {

  private final LogicalExpression expr;

  private final BiFunction<ColumnStatistic<C>, StatisticsProvider<C>, ParquetFilterPredicate.RowsMatch> predicate;

  private IsPredicate(LogicalExpression expr,
                      BiFunction<ColumnStatistic<C>, StatisticsProvider<C>, ParquetFilterPredicate.RowsMatch> predicate) {
    super(expr.getPosition());
    this.expr = expr;
    this.predicate = predicate;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    final List<LogicalExpression> args = new ArrayList<>();
    args.add(expr);
    return args.iterator();
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  /**
   * Apply the filter condition against the meta of the rowgroup.
   */
  @Override
  public ParquetFilterPredicate.RowsMatch matches(StatisticsProvider<C> evaluator) {
    ColumnStatistic<C> exprStat = expr.accept(evaluator, null);
    return isNullOrEmpty(exprStat) ? ParquetFilterPredicate.RowsMatch.SOME : predicate.apply(exprStat, evaluator);
  }

  /**
   * @param stat statistics object
   * @return <tt>true</tt> if the input stat object has valid statistics; false otherwise
   */
  static boolean isNullOrEmpty(ColumnStatistic stat) {
    return stat == null || stat.containsStatistic(() -> "rowCount")
        || stat.containsStatistic(() -> "minValue") || stat.containsStatistic(() -> "maxValue")
        || stat.containsStatistic(() -> "nullsCount");
  }

  /**
   * After the applying of the filter against the statistics of the rowgroup, if the result is RowsMatch.ALL,
   * then we still must know if the rowgroup contains some null values, because it can change the filter result.
   * If it contains some null values, then we change the RowsMatch.ALL into RowsMatch.SOME, which sya that maybe
   * some values (the null ones) should be disgarded.
   */
  private static ParquetFilterPredicate.RowsMatch checkNull(ColumnStatistic exprStat) {
    return hasNoNulls(exprStat) ? ParquetFilterPredicate.RowsMatch.ALL : ParquetFilterPredicate.RowsMatch.SOME;
  }

  /**
   * Checks that column chunk's statistics does not have nulls
   *
   * @param stat parquet column statistics
   * @return <tt>true</tt> if the parquet file does not have nulls and <tt>false</tt> otherwise
   */
  static boolean hasNoNulls(ColumnStatistic stat) {
    return (int) stat.getStatistic(() -> "rowCount") == 0;
  }

  /**
   * IS NULL predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createIsNullPredicate(LogicalExpression expr) {
    return new IsPredicate<C>(expr,
      (exprStat, evaluator) -> {
        // for arrays we are not able to define exact number of nulls
        // [1,2,3] vs [1,2] -> in second case 3 is absent and thus it's null but statistics shows no nulls
        if (expr instanceof TypedFieldExpr) {
          TypedFieldExpr typedFieldExpr = (TypedFieldExpr) expr;
          if (typedFieldExpr.getPath().isArray()) {
            return ParquetFilterPredicate.RowsMatch.SOME;
          }
        }
        if (hasNoNulls(exprStat)) {
          return ParquetFilterPredicate.RowsMatch.NONE;
        }
        return isAllNulls(exprStat, evaluator.getRowCount()) ? ParquetFilterPredicate.RowsMatch.ALL : ParquetFilterPredicate.RowsMatch.SOME;
      });
  }

  /**
   * Checks that column chunk's statistics has only nulls
   *
   * @param stat parquet column statistics
   * @param rowCount number of rows in the parquet file
   * @return <tt>true</tt> if all rows are null in the parquet file and <tt>false</tt> otherwise
   */
  static boolean isAllNulls(ColumnStatistic stat, long rowCount) {
    Preconditions.checkArgument(rowCount >= 0, String.format("negative rowCount %d is not valid", rowCount));
    return (int) stat.getStatistic(() -> "rowCount") == rowCount;
  }

  static boolean hasNonNullValues(ColumnStatistic stat) {
    return (int) stat.getStatistic(() -> "rowCount") > (int) stat.getStatistic(() -> "nullsCount");
  }

  /**
   * IS NOT NULL predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createIsNotNullPredicate(LogicalExpression expr) {
    return new IsPredicate<C>(expr,
      (exprStat, evaluator) -> isAllNulls(exprStat, evaluator.getRowCount()) ? ParquetFilterPredicate.RowsMatch.NONE : checkNull(exprStat)
    );
  }

  /**
   * IS TRUE predicate.
   */
  private static LogicalExpression createIsTruePredicate(LogicalExpression expr) {
    return new IsPredicate<Boolean>(expr, (exprStat, evaluator) -> {
      if (isAllNulls(exprStat, evaluator.getRowCount())) {
        return ParquetFilterPredicate.RowsMatch.NONE;
      }
      if (!hasNonNullValues(exprStat)) {
        return ParquetFilterPredicate.RowsMatch.SOME;
      }
      if (!((BooleanStatistics) exprStat).getMax()) {
        return ParquetFilterPredicate.RowsMatch.NONE;
      }
      return ((BooleanStatistics) exprStat).getMin() ? checkNull(exprStat) : ParquetFilterPredicate.RowsMatch.SOME;
    });
  }

  /**
   * IS FALSE predicate.
   */
  private static LogicalExpression createIsFalsePredicate(LogicalExpression expr) {
    return new IsPredicate<Boolean>(expr, (exprStat, evaluator) -> {
      if (isAllNulls(exprStat, evaluator.getRowCount())) {
        return ParquetFilterPredicate.RowsMatch.NONE;
      }
      if (!hasNonNullValues(exprStat)) {
        return ParquetFilterPredicate.RowsMatch.SOME;
      }
      if (((BooleanStatistics) exprStat).getMin()) {
        return ParquetFilterPredicate.RowsMatch.NONE;
      }
      return ((BooleanStatistics) exprStat).getMax() ? ParquetFilterPredicate.RowsMatch.SOME : checkNull(exprStat);
    });
  }

  /**
   * IS NOT TRUE predicate.
   */
  private static LogicalExpression createIsNotTruePredicate(LogicalExpression expr) {
    return new IsPredicate<Boolean>(expr, (exprStat, evaluator) -> {
      if (isAllNulls(exprStat, evaluator.getRowCount())) {
        return ParquetFilterPredicate.RowsMatch.ALL;
      }
      if (!hasNonNullValues(exprStat)) {
        return ParquetFilterPredicate.RowsMatch.SOME;
      }
      if (((BooleanStatistics) exprStat).getMin()) {
        return hasNoNulls(exprStat) ? ParquetFilterPredicate.RowsMatch.NONE : ParquetFilterPredicate.RowsMatch.SOME;
      }
      return ((BooleanStatistics) exprStat).getMax() ? ParquetFilterPredicate.RowsMatch.SOME : ParquetFilterPredicate.RowsMatch.ALL;
    });
  }

  /**
   * IS NOT FALSE predicate.
   */
  private static LogicalExpression createIsNotFalsePredicate(LogicalExpression expr) {
    return new IsPredicate<Boolean>(expr, (exprStat, evaluator) -> {
      if (isAllNulls(exprStat, evaluator.getRowCount())) {
        return ParquetFilterPredicate.RowsMatch.ALL;
      }
      if (!hasNonNullValues(exprStat)) {
        return ParquetFilterPredicate.RowsMatch.SOME;
      }
      if (!((BooleanStatistics) exprStat).getMax()) {
        return hasNoNulls(exprStat) ? ParquetFilterPredicate.RowsMatch.NONE : ParquetFilterPredicate.RowsMatch.SOME;
      }
      return ((BooleanStatistics) exprStat).getMin() ? ParquetFilterPredicate.RowsMatch.ALL : ParquetFilterPredicate.RowsMatch.SOME;
    });
  }

  public static <C extends Comparable<C>> LogicalExpression createIsPredicate(String function, LogicalExpression expr) {
    switch (function) {
      case FunctionGenerationHelper.IS_NULL:
        return IsPredicate.<C>createIsNullPredicate(expr);
      case FunctionGenerationHelper.IS_NOT_NULL:
        return IsPredicate.<C>createIsNotNullPredicate(expr);
      case FunctionGenerationHelper.IS_TRUE:
        return createIsTruePredicate(expr);
      case FunctionGenerationHelper.IS_NOT_TRUE:
        return createIsNotTruePredicate(expr);
      case FunctionGenerationHelper.IS_FALSE:
        return createIsFalsePredicate(expr);
      case FunctionGenerationHelper.IS_NOT_FALSE:
        return createIsNotFalsePredicate(expr);
      default:
        logger.warn("Unhandled IS function. Function name: {}", function);
        return null;
    }
  }
}
