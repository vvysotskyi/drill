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
package org.apache.drill.exec.planner.physical.visitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.physical.MetadataControllerPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectAllowDupPrel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.ScreenPrel;
import org.apache.drill.exec.planner.physical.WriterPrel;
import org.apache.drill.exec.planner.physical.UnnestPrel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class StarColumnConverter extends BasePrelVisitor<Prel, Void, RuntimeException> {

  private static final AtomicLong tableNumber = new AtomicLong(0);

  private boolean prefixedForStar;
  private boolean prefixedForWriter;

  private StarColumnConverter() {
  }

  public static Prel insertRenameProject(Prel root) {
    // Prefixing columns for columns expanded from star column. There are two cases.
    // 1. star is used in SELECT only.
    //   - Insert project under screen (PUS) to remove prefix,
    //   - Insert project above scan (PAS) to add prefix.
    // 2. star is used in CTAS
    //   - Insert project under Writer (PUW) to remove prefix
    //   - Insert project above scan (PAS) to add prefix.
    //   - DO NOT insert any project for prefix handling under Screen.

    // The following condtions should apply when prefix is required
    //   Any non-SCAN prel produces regular column / expression AND star column, multiple star columns.
    // This is because we have to use prefix to distinguish columns expanded
    // from star column, from those regular column referenced in the query.

    return root.accept(new StarColumnConverter(), null);
  }

  @Override
  public Prel visitScreen(ScreenPrel prel, Void value) throws RuntimeException {
    Prel child = ((Prel) prel.getInput(0)).accept(this, null);

    if (prefixedForStar) {
      if (prefixedForWriter) {
        // Prefix is added under CTAS Writer. We need create a new Screen with the converted child.
        return prel.copy(prel.getTraitSet(), Collections.singletonList(child));
      } else {
        // Prefix is added for SELECT only, not for CTAS writer.
        return insertProjUnderScreenOrWriter(prel, prel.getInput().getRowType(), child);
      }
    } else {
      // No prefix is
      return prel;
    }
  }

  // Note: the logic of handling * column for Writer is moved to ProjectForWriterVisitor.

  @Override
  public Prel visitWriter(WriterPrel prel, Void value) throws RuntimeException {
    RelNode child = ((Prel) prel.getInput(0)).accept(this, null);
    if (prefixedForStar) {
      prefixedForWriter = true;
      // return insertProjUnderScreenOrWriter(prel, prel.getInput().getRowType(), child);
      return prel.copy(prel.getTraitSet(), Collections.singletonList(child));
    } else {
      return prel;
    }
  }

  // insert PUS or PUW: Project Under Screen/Writer, when necessary.
  private Prel insertProjUnderScreenOrWriter(Prel prel, RelDataType origRowType, Prel child) {
    RexBuilder rexBuilder = child.getCluster().getRexBuilder();
    List<RelDataTypeField> fieldList = origRowType.getFieldList();
    List<RexNode> exprs = IntStream.range(0, origRowType.getFieldCount())
        .mapToObj(idx -> rexBuilder.makeInputRef(fieldList.get(idx).getType(), idx))
        .collect(Collectors.toList());

    RelDataType newRowType = RexUtil.createStructType(child.getCluster().getTypeFactory(),
        exprs, origRowType.getFieldNames(), null);

    int fieldCount = prel.getRowType().isStruct() ? prel.getRowType().getFieldCount() : 1;

    // Insert PUS/PUW : remove the prefix and keep the original field name.
    ProjectPrel proj;
    if (fieldCount > 1) { // no point in allowing duplicates if we only have one column
      proj = new ProjectAllowDupPrel(child.getCluster(),
          child.getTraitSet(),
          child,
          exprs,
          newRowType,
          true); //outputProj = true : will allow to build the schema for PUS Project, see ProjectRecordBatch#handleNullInput()
    } else {
      proj = new ProjectPrel(child.getCluster(),
          child.getTraitSet(),
          child,
          exprs,
          newRowType,
          true); //outputProj = true : will allow to build the schema for PUS Project, see ProjectRecordBatch#handleNullInput()
    }

    return (Prel) prel.copy(prel.getTraitSet(), Collections.singletonList(proj));
  }

  @Override
  public Prel visitProject(ProjectPrel prel, Void value) throws RuntimeException {
    // Require prefix rename : there exists other expression, in addition to a star column.
    if (!prefixedForStar  // not set yet.
        && StarColumnHelper.containsStarColumnInProject(prel.getInput().getRowType(), prel.getProjects())
        && prel.getRowType().getFieldNames().size() > 1) {
      prefixedForStar = true;
    }

    // For project, we need make sure that the project's field name is same as the input,
    // when the project expression is RexInPutRef, since we may insert a PAS which will
    // rename the projected fields.

    Prel child = ((Prel) prel.getInput(0)).accept(this, null);

    List<String> fieldNames = Lists.newArrayList();

    for (Pair<String, RexNode> pair : Pair.zip(prel.getRowType().getFieldNames(), prel.getProjects())) {
      if (pair.right instanceof RexInputRef) {
        String name = child.getRowType().getFieldNames().get(((RexInputRef) pair.right).getIndex());
        fieldNames.add(name);
      } else {
        fieldNames.add(pair.left);
      }
    }

    // Make sure the field names are unique : no allow of duplicate field names in a rowType.
    fieldNames = makeUniqueNames(fieldNames);

    RelDataType rowType = RexUtil.createStructType(prel.getCluster().getTypeFactory(),
        prel.getProjects(), fieldNames, null);

    ProjectPrel newProj = (ProjectPrel) prel.copy(prel.getTraitSet(), child, prel.getProjects(), rowType);

    if (ProjectRemoveRule.isTrivial(newProj)) {
      return child;
    } else {
      return newProj;
    }
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    if (prel instanceof MetadataControllerPrel) {
      // disallow renaming projections for analyze command
      return prel;
    }
    // Require prefix rename : there exists other expression, in addition to a star column.
    if (!prefixedForStar  // not set yet.
        && StarColumnHelper.containsStarColumn(prel.getRowType())
        && prel.getRowType().getFieldNames().size() > 1) {
      prefixedForStar = true;
    }

    List<RelNode> children = Lists.newArrayList();
    for (Prel child : prel) {
      child = child.accept(this, null);
      children.add(child);
    }

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  private Prel prefixTabNameToStar(Prel prel, Void value) throws RuntimeException {
    if (prefixedForStar && StarColumnHelper.containsStarColumn(prel.getRowType())) {

      List<RexNode> exprs = Lists.newArrayList();

      for (RelDataTypeField field : prel.getRowType().getFieldList()) {
        RexNode expr = prel.getCluster().getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        exprs.add(expr);
      }

      List<String> fieldNames = Lists.newArrayList();

      long tableId = tableNumber.getAndIncrement();

      for (String name : prel.getRowType().getFieldNames()) {
        if (StarColumnHelper.isNonPrefixedStarColumn(name)) {
          fieldNames.add("T" + tableId + StarColumnHelper.PREFIX_DELIMITER + name);  // Add prefix to * column.
        } else {
          fieldNames.add(name);  // Keep regular column as it is.
        }
      }
      RelDataType rowType = RexUtil.createStructType(prel.getCluster().getTypeFactory(),
          exprs, fieldNames, null);

      // insert a PAS.
      ProjectPrel proj = new ProjectPrel(prel.getCluster(), prel.getTraitSet(), prel, exprs, rowType);

      return proj;
    } else {
      return visitPrel(prel, value);
    }
  }

  @Override
  public Prel visitScan(ScanPrel scanPrel, Void value) throws RuntimeException {
    return prefixTabNameToStar(scanPrel, value);
  }

  @Override
  public Prel visitUnnest(UnnestPrel unnestPrel, Void value) throws RuntimeException {
    return prefixTabNameToStar(unnestPrel, value);
  }

  /**
   * Walks through names and if finds the duplicate name, replaces it with
   * new unique name.
   *
   * @param names list of names with possible duplicates
   * @return list of names where duplicates are replaced by generated names.
   */
  private List<String> makeUniqueNames(List<String> names) {
    Set<String> originalNames = new HashSet<>(names);
    if (names.size() == originalNames.size()) {
      return names;
    }
    Set<String> uniqueNames = new LinkedHashSet<>();
    for (String s : names) {
      for (int i = 0; uniqueNames.contains(s) || (i > 0 && originalNames.contains(s)); i++) {
        s += i;
      }
      uniqueNames.add(s);
    }
    return new ArrayList<>(uniqueNames);
  }
}
