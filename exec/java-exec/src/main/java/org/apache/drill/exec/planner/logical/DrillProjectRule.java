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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.function.Predicate;

/**
 * Rule that converts a {@link org.apache.calcite.rel.logical.LogicalProject} to a Drill "project" operation.
 */
public class DrillProjectRule extends ConverterRule {
  public static final RelOptRule INSTANCE = new DrillProjectRule();

  private DrillProjectRule() {
    super(LogicalProject.class, (Predicate<RelNode>) r -> true, Convention.NONE, DrillRel.DRILL_LOGICAL,
        DrillRelFactories.LOGICAL_BUILDER, "DrillProjectRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Project project = (Project) rel;
    RelNode input = project.getInput();
    RelTraitSet traits = project.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    RelNode convertedInput = convert(input, input.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());
    return new DrillProjectRel(
        project.getCluster(), traits, convertedInput, project.getProjects(), project.getRowType());
  }
}
