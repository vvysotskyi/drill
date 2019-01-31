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
package org.apache.drill.exec.store.parquet.stat;

import org.apache.drill.common.types.TypeProtos;
import org.apache.parquet.column.statistics.Statistics;

/**
 * Wrapper for the Parquet column statistics. Additionally involves Drill's MajorType
 * TODO: get rid from this wrapper. Use the common one for the all table formats
 *
 * @param <T> the Java type described by this Statistics instance
 */
public class ColumnStatistics<T extends Comparable<T>> {
  private final Statistics<T> statistics;
  private final TypeProtos.MajorType majorType;

  public ColumnStatistics(final Statistics<T> statistics, final TypeProtos.MajorType majorType) {
    this.statistics = statistics;
    this.majorType = majorType;
  }

  public Statistics<T> getStatistics() {
    return this.statistics;
  }

  public TypeProtos.MajorType getMajorType() {
    return this.majorType;
  }

}
