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
package org.apache.drill.exec.physical.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Iterators;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.BaseMetadataGroupScan;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyGroupScan;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.BlockMapBuilder;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.util.ImpersonationUtil;

import java.io.IOException;
import java.util.List;

public class FileMetadataGroupScan extends BaseMetadataGroupScan {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyGroupScan.class);

  private FileSelection selection;
  private EasyFormatPlugin<?> formatPlugin;
  private int maxWidth;

  private ListMultimap<Integer, CompleteFileWork> mappings;
  private List<CompleteFileWork> chunks;
  private List<EndpointAffinity> endpointAffinities;
  private String selectionRoot;

  protected FileMetadataGroupScan(String userName, List<SchemaPath> columns, LogicalExpression filter) {
    super(userName, columns, filter);
  }

  @JsonIgnore
  public Iterable<CompleteFileWork> getWorkIterable() {
    return () -> Iterators.unmodifiableIterator(chunks.iterator());
  }

  private FileMetadataGroupScan(final FileMetadataGroupScan that) {
    super(that.getUserName(), that.columns, that.filter);
    selection = that.selection;
    formatPlugin = that.formatPlugin;
    columns = that.columns;
    selectionRoot = that.selectionRoot;
    chunks = that.chunks;
    endpointAffinities = that.endpointAffinities;
    maxWidth = that.maxWidth;
    mappings = that.mappings;
  }

  private void initFromSelection(FileSelection selection, EasyFormatPlugin<?> formatPlugin) throws IOException {
    @SuppressWarnings("resource")
    final DrillFileSystem dfs = ImpersonationUtil.createFileSystem(getUserName(), formatPlugin.getFsConf());
    this.selection = selection;
    BlockMapBuilder b = new BlockMapBuilder(dfs, formatPlugin.getContext().getBits());
    this.chunks = b.generateFileWork(selection.getStatuses(dfs), formatPlugin.isBlockSplittable());
    this.maxWidth = chunks.size();
    this.endpointAffinities = AffinityCreator.getAffinityMap(chunks);
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return maxWidth;
  }


  @Override
  public ScanStats getScanStats(final PlannerSettings settings) {
    // old
    long data = 0;
    for (final CompleteFileWork work : getWorkIterable()) {
      data += work.getTotalBytes();
    }

    final long estRowCount = data / 1024;
    ScanStats oldScanStats = new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, estRowCount, 1, data);


    if (tableMetadata == null) {
      return oldScanStats;
    }
    long rowCount = (long) tableMetadata.getStatistic(() -> "rowCount");
    return new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, rowCount, 1, oldScanStats.getDiskCost());
  }

  @Override
  public boolean hasFiles() {
    return true;
  }

  @JsonProperty("files")
  @Override
  public List<String> getFiles() {
    return selection.getFiles();
  }

  @JsonIgnore
  public FileSelection getFileSelection() {
    return selection;
  }

  @Override
  public void modifyFileSelection(FileSelection selection) {
    this.selection = selection;
  }

  @Override
  protected void initInternal() throws IOException {

  }

//  @Override
//  protected BaseMetadataGroupScan cloneWithFileSet(Collection<FileMetadata> files) throws IOException {
//    return null;
//  }

  @Override
  protected boolean supportsFileImplicitColumns() {
    return false;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return new FileMetadataGroupScan(this);
  }


  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (endpointAffinities == null) {
      logger.debug("chunks: {}", chunks.size());
      endpointAffinities = AffinityCreator.getAffinityMap(chunks);
    }
    return endpointAffinities;
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> incomingEndpoints) {
    mappings = AssignmentCreator.getMappings(incomingEndpoints, chunks);
  }

  private void createMappings(List<EndpointAffinity> affinities) {
    List<CoordinationProtos.DrillbitEndpoint> endpoints = Lists.newArrayList();
    for (EndpointAffinity e : affinities) {
      endpoints.add(e.getEndpoint());
    }
    this.applyAssignments(endpoints);
  }

  @Override
  public EasySubScan getSpecificScan(int minorFragmentId) {
    if (mappings == null) {
      createMappings(this.endpointAffinities);
    }
    assert minorFragmentId < mappings.size() : String.format(
      "Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.size(),
      minorFragmentId);

    List<CompleteFileWork> filesForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!filesForMinor.isEmpty(),
      String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    EasySubScan subScan = new EasySubScan(getUserName(), convert(filesForMinor), formatPlugin, columns, selectionRoot);
    subScan.setOperatorId(this.getOperatorId());
    return subScan;
  }

  private List<CompleteFileWork.FileWorkImpl> convert(List<CompleteFileWork> list) {
    List<CompleteFileWork.FileWorkImpl> newList = Lists.newArrayList();
    for (CompleteFileWork f : list) {
      newList.add(f.getAsFileWork());
    }
    return newList;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig() {
    return formatPlugin.getConfig();
  }

  @Override
  public String toString() {
    final String pattern = "EasyGroupScan [selectionRoot=%s, numFiles=%s, columns=%s, files=%s]";
    return String.format(pattern, selectionRoot, getFiles().size(), columns, getFiles());
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  protected GroupScanBuilder getBuilder() {
    return new FileMetadataGroupScanBuilder(this);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    if (!formatPlugin.supportsPushDown()) {
      throw new IllegalStateException(String.format("%s doesn't support pushdown.", this.getClass().getSimpleName()));
    }
    FileMetadataGroupScan newScan = new FileMetadataGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public FileGroupScan clone(FileSelection selection) throws IOException {
    FileMetadataGroupScan newScan = new FileMetadataGroupScan(this);
    newScan.initFromSelection(selection, formatPlugin);
    newScan.mappings = null; /* the mapping will be created later when we get specific scan
                                since the end-point affinities are not known at this time */
    return newScan;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return formatPlugin.supportsPushDown();
  }

  private static class FileMetadataGroupScanBuilder extends GroupScanBuilder {
    FileMetadataGroupScan source;

    public FileMetadataGroupScanBuilder(FileMetadataGroupScan source) {
      this.source = source;
    }

    @Override
    public BaseMetadataGroupScan build() {
      FileMetadataGroupScan groupScan = new FileMetadataGroupScan(source.getUserName(), source.getColumns(), source.getFilter());
      groupScan.tableMetadata = tableMetadata.get(0);
      groupScan.partitions = partitions;
      groupScan.files = files;

      return groupScan;
    }
  }
}
