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
package org.apache.drill.exec.planner.physical;


import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;

public class PrelFactories {
  public static final RelFactories.ProjectFactory PROJECT_FACTORY =
      new DrillProjectPrelFactory();

  /**
   * Implementation of {@link RelFactories.ProjectFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  private static class DrillProjectPrelFactory implements RelFactories.ProjectFactory {

    @Override
    public RelNode createProject(RelNode input, List<RelHint> hints,
                                 List<? extends RexNode> childExprs, List<String> fieldNames) {
      RelOptCluster cluster = input.getCluster();
      RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), childExprs, fieldNames, null);
      RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
      ArrayList<RexNode> exps = new ArrayList<>(childExprs);
      RelNode project = new ProjectPrel(cluster, traits, input, exps, rowType);
      return project;
    }
  }
}
