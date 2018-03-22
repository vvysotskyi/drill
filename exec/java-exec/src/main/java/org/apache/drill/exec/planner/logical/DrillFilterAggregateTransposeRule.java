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

package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.drill.exec.planner.DrillRelBuilder;

public class DrillFilterAggregateTransposeRule extends FilterAggregateTransposeRule{

  // Since this rule creates logical rel nodes, it should be applied to logical rel nodes only
  public static final FilterAggregateTransposeRule INSTANCE = new DrillFilterAggregateTransposeRule();

  private DrillFilterAggregateTransposeRule() {
    super(LogicalFilter.class, DrillRelBuilder.proto(Contexts.of(RelFactories.DEFAULT_FILTER_FACTORY)),
        LogicalAggregate.class);
  }
}
