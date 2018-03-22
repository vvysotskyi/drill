/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;

/**
 * Rule that can be applied to a
 * {@link org.apache.calcite.rel.logical.LogicalProject} and that produces, in turn,
 * a mixture of {@code LogicalProject}
 * and {@link org.apache.calcite.rel.logical.LogicalWindow}.
 */
public class DrillProjectToWindowRule extends ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule {

  public static final ProjectToWindowRule PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW_RULE =
      new DrillProjectToWindowRule(DrillRelFactories.LOGICAL_BUILDER);

  public DrillProjectToWindowRule(RelBuilderFactory relBuilderFactory) {
    super(relBuilderFactory);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return DrillRelOptUtil.allRelsLogical(ImmutableList.of(call.rel(0)));
  }
}
