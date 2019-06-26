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
package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.PhysicalOperator;

import java.util.Collections;
import java.util.List;

@JsonTypeName("metadataAggregate")
public class MetadataAggPOP extends StreamingAggregate {
  private final List<SchemaPath> interestingColumns;
  private final boolean createNewAggregations;
  private final List<SchemaPath> excludedColumns;

  @JsonCreator
  public MetadataAggPOP(@JsonProperty("child") PhysicalOperator child,
      @JsonProperty("keys") List<NamedExpression> keys,
      @JsonProperty("interestingColumns") List<SchemaPath> interestingColumns,
      @JsonProperty("createNewAggregations") boolean createNewAggregations,
      @JsonProperty("excludedColumns") List<SchemaPath> excludedColumns) {
    super(child, keys, Collections.emptyList());
    this.interestingColumns = interestingColumns;
    this.createNewAggregations = createNewAggregations;
    this.excludedColumns = excludedColumns;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new MetadataAggPOP(child, getKeys(), interestingColumns, createNewAggregations, excludedColumns);
  }

  public List<SchemaPath> getInterestingColumns() {
    return interestingColumns;
  }

  @JsonProperty("createNewAggregations")
  public boolean createNewAggregations() {
    return createNewAggregations;
  }

  public List<SchemaPath> getExcludedColumns() {
    return excludedColumns;
  }
}
