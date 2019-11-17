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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.metastore.analyze.MetadataAggregateContext;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.MetadataAggRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class MetadataAggPrule extends Prule {
  public static final MetadataAggPrule INSTANCE = new MetadataAggPrule();

  public MetadataAggPrule() {
    super(RelOptHelper.any(MetadataAggRel.class, DrillRel.DRILL_LOGICAL),
        "MetadataAggPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    MetadataAggRel relNode = call.rel(0);
    RelNode input = relNode.getInput();

    RelTraitSet traits;

    try {
      if (/*relNode.getContext().groupByExpressions().isEmpty()*/true) {
        DrillDistributionTrait singleDist = DrillDistributionTrait.SINGLETON;
        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(singleDist);
        createTransformRequest(call, relNode, input, traits);
      } else {
        // hash distribute on all grouping keys
        DrillDistributionTrait distOnAllKeys =
            new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
                ImmutableList.copyOf(getDistributionField(relNode, true /* get all grouping keys */)));

        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distOnAllKeys);
        createTransformRequest(call, relNode, input, traits);

        // hash distribute on single grouping key
        DrillDistributionTrait distOnOneKey =
            new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
                ImmutableList.copyOf(getDistributionField(relNode, false /* get single grouping key */)));

        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distOnOneKey);
        createTransformRequest(call, relNode, input, traits);

        if (/*create2PhasePlan(call, relNode)*/false) {
          traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL);

          RelNode convertedInput = convert(input, traits);
          new TwoPhaseSubset(call, distOnAllKeys).go(relNode, convertedInput);

        }
      }
    } catch (InvalidRelException e) {
      throw new DrillRuntimeException(e);
    }
  }

  private class TwoPhaseSubset extends SubsetTransformer<MetadataAggRel, InvalidRelException> {
    final RelTrait distOnAllKeys;

    public TwoPhaseSubset(RelOptRuleCall call, RelTrait distOnAllKeys) {
      super(call);
      this.distOnAllKeys = distOnAllKeys;
    }

    @Override
    public RelNode convertChild(MetadataAggRel aggregate, RelNode input) throws InvalidRelException {

      RelTraitSet traits = newTraitSet(Prel.DRILL_PHYSICAL, input.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE));
      RelNode newInput = convert(input, traits);

      MetadataAggPrel phase1Agg = new MetadataAggPrel(
          aggregate.getCluster(),
          traits,
          newInput,
          aggregate.getContext(),
          AggPrelBase.OperatorPhase.PHASE_1of2);

      HashToRandomExchangePrel exch =
          new HashToRandomExchangePrel(phase1Agg.getCluster(), phase1Agg.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distOnAllKeys),
              phase1Agg, ImmutableList.copyOf(getDistributionField(aggregate, true)));

//      ImmutableBitSet newGroupSet = remapGroupSet(aggregate.getGroupSet());
//      List<ImmutableBitSet> newGroupSets = new ArrayList<>();
//      for (ImmutableBitSet groupSet : aggregate.getGroupSets()) {
//        newGroupSets.add(remapGroupSet(groupSet));
//      }

      MetadataAggregateContext aggContext = aggregate.getContext().toBuilder()
          .createNewAggregations(false)
          .build();
      return new MetadataAggPrel(
          aggregate.getCluster(),
          exch.getTraitSet(),
          exch,
          aggContext,
          AggPrelBase.OperatorPhase.PHASE_2of2);
    }
  }

  private void createTransformRequest(RelOptRuleCall call, MetadataAggRel aggregate,
      RelNode input, RelTraitSet traits) {

    final RelNode convertedInput = convert(input, PrelUtil.fixTraits(call, traits));

    MetadataAggPrel newAgg = new MetadataAggPrel(
        aggregate.getCluster(),
        traits,
        convertedInput,
        aggregate.getContext(),
        AggPrelBase.OperatorPhase.PHASE_1of1);

    call.transformTo(newAgg);
  }
  protected List<DrillDistributionTrait.DistributionField> getDistributionField(MetadataAggRel rel, boolean allFields) {
    List<DrillDistributionTrait.DistributionField> groupByFields = new ArrayList<>();
    int groupByExprsSize = rel.getContext().groupByExpressions().size();

    for (int group = 1; group <= groupByExprsSize; group++) {
      DrillDistributionTrait.DistributionField field = new DrillDistributionTrait.DistributionField(group);
      groupByFields.add(field);

      if (!allFields && groupByFields.size() == 1) {
        // TODO: if we are only interested in 1 grouping field, pick the first one for now..
        // but once we have num distinct values (NDV) statistics, we should pick the one
        // with highest NDV.
        break;
      }
    }

    return groupByFields;
  }

}
