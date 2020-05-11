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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.schema.Table;

import java.util.Collections;
import java.util.List;

/**
 * This class puts the file selection string into it's digest.
 * When directory-based partition pruning applied, file selection could be different for the same
 * table.
 */
public class DrillDirPrunedScanRel extends TableScan {
  private final String digestFromSelection;

  public DrillDirPrunedScanRel(RelOptCluster cluster, RelTraitSet traitSet,
                               RelOptTable table, String digestFromSelection) {
    super(cluster, traitSet, Collections.emptyList(), table);
    this.digestFromSelection = digestFromSelection;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillDirPrunedScanRel(getCluster(), traitSet, table, digestFromSelection);
  }

  /** Creates an DrillDirPrunedScanRel. */
  public static TableScan create(RelOptCluster cluster, RelOptTable relOptTable, String digestFromSelection) {
    RelTraitSet traitSet = getRelTraits(cluster, relOptTable);
    return new DrillDirPrunedScanRel(cluster, traitSet, relOptTable, digestFromSelection);
  }

  private static RelTraitSet getRelTraits(RelOptCluster cluster, RelOptTable relOptTable) {
    Table table = relOptTable.unwrap(Table.class);
    return cluster.traitSetOf(Convention.NONE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> table != null ? table.getStatistic().getCollations() : Collections.emptyList());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("selection", this.digestFromSelection);
  }

}
