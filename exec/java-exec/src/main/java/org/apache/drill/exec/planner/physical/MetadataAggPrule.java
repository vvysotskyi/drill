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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.MetadataAggRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.NamedDistributionField;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MetadataAggPrule extends Prule {
  public static final MetadataAggPrule INSTANCE = new MetadataAggPrule();

  public MetadataAggPrule() {
    super(RelOptHelper.any(MetadataAggRel.class, DrillRel.DRILL_LOGICAL),
        "MetadataAggPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    MetadataAggRel aggregate = call.rel(0);
    RelNode input = aggregate.getInput();

    int groupByExprsSize = aggregate.getContext().groupByExpressions().size();

    List<RelFieldCollation> collations = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (int i = 0; i < groupByExprsSize; i++) {
      collations.add(new RelFieldCollation(i + 1));
      names.add(((SchemaPath)aggregate.getContext().groupByExpressions().get(i).getExpr()).getRootSegmentPath());
    }

    RelCollation collation = new NamedRelCollation(collations, names);

    RelTraitSet traits;

    try {
      if (aggregate.getContext().groupByExpressions().isEmpty()) {
        DrillDistributionTrait singleDist = DrillDistributionTrait.SINGLETON;
        RelTraitSet singleDistTrait = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(singleDist);

        createTransformRequest(call, aggregate, input, singleDistTrait);
      } else {
        // hash distribute on all grouping keys
        DrillDistributionTrait distOnAllKeys =
            new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
                ImmutableList.copyOf(getDistributionField(aggregate)));

        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation).plus(DrillDistributionTrait.SINGLETON);
        // TODO: DRILL-7433 - uncomment this line when palatalization for MetadataHandler is implemented
        // traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation).plus(distOnAllKeys);
        if (!aggregate.getContext().createNewAggregations()) {
          createTransformRequest(call, aggregate, input, traits);
        }

        // force 2-phase aggregation for bottom aggregate call
        // to produce sort locally before aggregation is produced
        if (aggregate.getContext().createNewAggregations()) {
          traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL);
          RelNode convertedInput = convert(input, traits);

          new TwoPhaseMetadataAggSubsetTransformer(call, collation, distOnAllKeys)
              .go(aggregate, convertedInput);
        }
      }
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
  }

  private void createTransformRequest(RelOptRuleCall call, MetadataAggRel aggregate,
      RelNode input, RelTraitSet traits) {

    RelNode convertedInput = convert(input, PrelUtil.fixTraits(call, traits));

    MetadataAggPrel newAgg = new MetadataAggPrel(
        aggregate.getCluster(),
        traits,
        convertedInput,
        aggregate.getContext(),
        AggPrelBase.OperatorPhase.PHASE_1of1);

    call.transformTo(newAgg);
  }

  private static List<NamedDistributionField> getDistributionField(MetadataAggRel rel) {
    List<NamedDistributionField> groupByFields = new ArrayList<>();
    int groupByExprsSize = rel.getContext().groupByExpressions().size();

    for (int group = 1; group <= groupByExprsSize; group++) {
      SchemaPath fieldName = (SchemaPath) rel.getContext().groupByExpressions().get(group - 1).getExpr();
      NamedDistributionField field =
          new NamedDistributionField(group, fieldName.getRootSegmentPath());
      groupByFields.add(field);
    }

    return groupByFields;
  }

  /**
   * Implementation of {@link RelCollationImpl} with field name.
   * Stores {@link RelFieldCollation} list and corresponding field names to be used in sort operators.
   * Field name is required for the case of dynamic schema discovering
   * when field is not present in rel data type at planning time
   */
  public static class NamedRelCollation extends RelCollationImpl {
    private final List<String> names;

    protected NamedRelCollation(List<RelFieldCollation> fieldCollations, List<String> names) {
      super(com.google.common.collect.ImmutableList.copyOf(fieldCollations));
      this.names = Collections.unmodifiableList(names);
    }

    public String getName(int collationIndex) {
      return names.get(collationIndex - 1);
    }
  }

  /**
   * {@link SubsetTransformer} for creating two-phase metadata aggregation.
   */
  private static class TwoPhaseMetadataAggSubsetTransformer
      extends SubsetTransformer<MetadataAggRel, InvalidRelException> {

    private final RelCollation collation;
    private final DrillDistributionTrait distributionTrait;

    public TwoPhaseMetadataAggSubsetTransformer(RelOptRuleCall call,
        RelCollation collation, DrillDistributionTrait distributionTrait) {
      super(call);
      this.collation = collation;
      this.distributionTrait = distributionTrait;
    }

    @Override
    public RelNode convertChild(MetadataAggRel aggregate, RelNode child) {
      DrillDistributionTrait toDist = child.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE);
      RelTraitSet traits = newTraitSet(Prel.DRILL_PHYSICAL, collation, toDist);
      RelNode newInput = convert(child, traits);

      MetadataAggPrel phase1Agg = new MetadataAggPrel(
          aggregate.getCluster(),
          traits,
          newInput,
          aggregate.getContext(),
          AggPrelBase.OperatorPhase.PHASE_1of2);

      int numEndPoints = PrelUtil.getSettings(phase1Agg.getCluster()).numEndPoints();

      HashToMergeExchangePrel exch =
          new HashToMergeExchangePrel(phase1Agg.getCluster(),
              phase1Agg.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distributionTrait),
              phase1Agg,
              ImmutableList.copyOf(getDistributionField(aggregate)),
              collation,
              numEndPoints);

      return new MetadataAggPrel(
          aggregate.getCluster(),
          newTraitSet(Prel.DRILL_PHYSICAL, collation, DrillDistributionTrait.SINGLETON),
          exch,
          aggregate.getContext(),
          AggPrelBase.OperatorPhase.PHASE_2of2);
    }
  }
}
