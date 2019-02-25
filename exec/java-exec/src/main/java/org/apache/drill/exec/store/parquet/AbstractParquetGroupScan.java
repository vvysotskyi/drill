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
package org.apache.drill.exec.store.parquet;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.expression.ExpressionStringBuilder;
import org.apache.drill.exec.physical.base.AbstractGroupScanWithMetadata;
import org.apache.drill.exec.physical.base.ParquetMetadataProvider;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.metastore.LocationProvider;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.TableStatisticsKind;
import org.apache.drill.metastore.expr.StatisticsConstants;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.metastore.expr.FilterPredicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractParquetGroupScan extends AbstractGroupScanWithMetadata {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractParquetGroupScan.class);

  protected List<ReadEntryWithPath> entries;
  protected List<RowGroupMetadata> rowGroups;

  protected ListMultimap<Integer, RowGroupInfo> mappings;
  protected ParquetReaderConfig readerConfig;

  private List<EndpointAffinity> endpointAffinities;
  // used for applying assignments for incoming endpoints
  private List<RowGroupInfo> rowGroupInfos;

  protected AbstractParquetGroupScan(String userName,
                                     List<SchemaPath> columns,
                                     List<ReadEntryWithPath> entries,
                                     ParquetReaderConfig readerConfig,
                                     LogicalExpression filter) {
    super(userName, columns, filter);
    this.entries = entries;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
  }

  // immutable copy constructor
  protected AbstractParquetGroupScan(AbstractParquetGroupScan that) {
    super(that);

    this.rowGroups = that.rowGroups;

    this.endpointAffinities = that.endpointAffinities == null ? null : new ArrayList<>(that.endpointAffinities);
    this.mappings = that.mappings == null ? null : ArrayListMultimap.create(that.mappings);

    this.entries = that.entries == null ? null : new ArrayList<>(that.entries);
    this.readerConfig = that.readerConfig;

  }

  @Override
  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  @JsonProperty("readerConfig")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // do not serialize reader config if it contains all default values
  public ParquetReaderConfig getReaderConfigForSerialization() {
    return ParquetReaderConfig.getDefaultInstance().equals(readerConfig) ? null : readerConfig;
  }

  @JsonIgnore
  public ParquetReaderConfig getReaderConfig() {
    return readerConfig;
  }

  @Override
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  /**
   * Calculates the affinity each endpoint has for this scan,
   * by adding up the affinity each endpoint has for each rowGroup.
   *
   * @return a list of EndpointAffinity objects
   */
  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return endpointAffinities;
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> incomingEndpoints) {
    this.mappings = AssignmentCreator.getMappings(incomingEndpoints, getRowGroupInfos());
  }

  private List<RowGroupInfo> getRowGroupInfos() {
    if (rowGroupInfos == null) {
      Map<String, CoordinationProtos.DrillbitEndpoint> hostEndpointMap = new HashMap<>();

      for (CoordinationProtos.DrillbitEndpoint endpoint : getDrillbits()) {
        hostEndpointMap.put(endpoint.getAddress(), endpoint);
      }

      rowGroupInfos = new ArrayList<>();
      for (RowGroupMetadata rowGroupMetadata : getRowGroupsMetadata()) {
        RowGroupInfo rowGroupInfo = new RowGroupInfo(rowGroupMetadata.getLocation(),
            (long) rowGroupMetadata.getStatistic(() -> StatisticsConstants.START),
            (long) rowGroupMetadata.getStatistic(() -> StatisticsConstants.LENGTH),
            rowGroupMetadata.getRowGroupIndex(),
            (long) rowGroupMetadata.getStatistic(TableStatisticsKind.ROW_COUNT));
        rowGroupInfo.setNumRecordsToRead(rowGroupInfo.getRowCount());

        EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
        for (String host : rowGroupMetadata.getHostAffinity().keySet()) {
          if (hostEndpointMap.containsKey(host)) {
            endpointByteMap.add(hostEndpointMap.get(host),
              (long) (rowGroupMetadata.getHostAffinity().get(host) * (long) rowGroupMetadata.getStatistic(() -> "length")));
          }
        }

        rowGroupInfo.setEndpointByteMap(endpointByteMap);

        rowGroupInfos.add(rowGroupInfo);
      }
    }
    return rowGroupInfos;
  }

  @Override
  public int getMaxParallelizationWidth() {
    if (getRowGroupsMetadata() != null) {
      return getRowGroupsMetadata().size();
    } else {
      if (getFilesMetadata() != null) {
        return getFilesMetadata().size();
      } else {
        return getPartitionsMetadata() != null ? getPartitionsMetadata().size() : 1;
      }
    }
  }

  protected List<RowGroupReadEntry> getReadEntries(int minorFragmentId) {
    assert minorFragmentId < mappings.size() : String
        .format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.",
            mappings.size(), minorFragmentId);

    List<RowGroupInfo> rowGroupsForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!rowGroupsForMinor.isEmpty(),
        String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    List<RowGroupReadEntry> readEntries = new ArrayList<>();
    for (RowGroupInfo rgi : rowGroupsForMinor) {
      RowGroupReadEntry entry = new RowGroupReadEntry(rgi.getPath(), rgi.getStart(),
          rgi.getLength(), rgi.getRowGroupIndex(),
          rgi.getNumRecordsToRead());
      readEntries.add(entry);
    }
    return readEntries;
  }

  @Override
  public AbstractGroupScanWithMetadata applyFilter(LogicalExpression filterExpr, UdfUtilities udfUtilities,
      FunctionImplementationRegistry functionImplementationRegistry, OptionManager optionManager) {
    // Builds filter for pruning. If filter cannot be built, null should be returned.
    FilterPredicate filterPredicate = getFilterPredicate(filterExpr, udfUtilities, functionImplementationRegistry, optionManager, true);
    if (filterPredicate == null) {
      logger.debug("FilterPredicate cannot be built.");
      return null;
    }

    Set<SchemaPath> schemaPathsInExpr =
        filterExpr.accept(new FilterEvaluatorUtils.FieldReferenceFinder(), null);

    RowGroupScanFilterer builder = getFilterer().getFiltered(optionManager, filterPredicate, schemaPathsInExpr);

    if (getRowGroupsMetadata() != null) {
      if (builder.getRowGroups() != null && getRowGroupsMetadata().size() == builder.getRowGroups().size()) {
        // There is no reduction of files
        logger.debug("applyFilter() does not have any pruning");
        matchAllMetadata = builder.isMatchAllMetadata();
        return null;
      }
    } else if (getFilesMetadata() != null) {
      if (builder.getFiles() != null && getFilesMetadata().size() == builder.getFiles().size()) {
        // There is no reduction of files
        logger.debug("applyFilter() does not have any pruning");
        matchAllMetadata = builder.isMatchAllMetadata();
        return null;
      }
    } else if (getPartitionsMetadata() != null) {
      if (builder.getPartitions() != null && getPartitionsMetadata().size() == builder.getPartitions().size()) {
        // There is no reduction of partitions
        logger.debug("applyFilter() does not have any pruning ");
        matchAllMetadata = builder.isMatchAllMetadata();
        return null;
      }
    } else if (getTableMetadata() != null) {
      // There is no reduction
      logger.debug("applyFilter() does not have any pruning");
      matchAllMetadata = builder.isMatchAllMetadata();
      return null;
    }

    if (!builder.isMatchAllMetadata()
        // filter returns empty result using table metadata
        && ((builder.getTableMetadata() == null && getTableMetadata() != null)
            // all partitions pruned if partition metadata is available
            || CollectionUtils.isEmpty(builder.getPartitions()) && CollectionUtils.isNotEmpty(getPartitionsMetadata())
            // all files are pruned if file metadata is available
            || CollectionUtils.isEmpty(builder.getFiles()) && CollectionUtils.isNotEmpty(getFilesMetadata())
            // all row groups are pruned if row group metadata is available
            || CollectionUtils.isEmpty(builder.getRowGroups()) && CollectionUtils.isNotEmpty(getRowGroupsMetadata()))) {
      if (getRowGroupsMetadata().size() == 1) {
        // For the case when group scan has single row group and it was filtered,
        // no need to create new group scan with the same row group.
        return null;
      }
      logger.debug("All row groups have been filtered out. Add back one to get schema from scanner");
      builder.withRowGroups(getNextOrEmpty(getRowGroupsMetadata()))
          .withTable(getTableMetadata())
          .withPartitions(getNextOrEmpty(getPartitionsMetadata()))
          .withFiles(getNextOrEmpty(getFilesMetadata()))
          .withMatching(false);
    }

    if (builder.getOverflowLevel() != MetadataLevel.NONE) {
      logger.warn("applyFilter {} wasn't able to do pruning for  all metadata levels filter condition, since metadata count for " +
            "{} level exceeds `planner.store.parquet.rowgroup.filter.pushdown.threshold` value.\n" +
            "But underlying metadata was pruned without filter expression according to the metadata with above level.",
          ExpressionStringBuilder.toString(filterExpr), builder.getOverflowLevel());
    }

    logger.debug("applyFilter {} reduce row groups # from {} to {}",
        ExpressionStringBuilder.toString(filterExpr), getRowGroupsMetadata().size(), builder.getRowGroups().size());

    return builder.build();
  }

  @Override
  protected TupleMetadata getColumnMetadata() {
    TupleMetadata columnMetadata = super.getColumnMetadata();
    if (columnMetadata == null && CollectionUtils.isNotEmpty(getRowGroupsMetadata())) {
      return getRowGroupsMetadata().iterator().next().getSchema();
    }
    return columnMetadata;
  }

  // narrows the return type
  protected abstract RowGroupScanFilterer getFilterer();

  private List<RowGroupMetadata> pruneRowGroupsForFiles(List<FileMetadata> filteredFileMetadata) {
    List<RowGroupMetadata> prunedRowGroups = new ArrayList<>();
    for (RowGroupMetadata file : getRowGroupsMetadata()) {
      for (FileMetadata filteredPartition : filteredFileMetadata) {
        if (file.getLocation().startsWith(filteredPartition.getLocation())) {
          prunedRowGroups.add(file);
          break;
        }
      }
    }

    return prunedRowGroups;
  }

  // filter push down methods block end

  // limit push down methods start
  @Override
  public GroupScan applyLimit(int maxRecords) {
    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    RowGroupScanFilterer builder = (RowGroupScanFilterer) limitFiles(maxRecords);

    if (builder.getTableMetadata() == null && getTableMetadata() != null) {
      logger.debug("limit push down does not apply, since table has less rows.");
      return null;
    }

    List<FileMetadata> qualifiedFiles = builder.getFiles();
    qualifiedFiles = qualifiedFiles != null ? qualifiedFiles : Collections.emptyList();

    List<RowGroupMetadata> qualifiedRowGroups = !qualifiedFiles.isEmpty() ? pruneRowGroupsForFiles(qualifiedFiles.subList(0, qualifiedFiles.size() - 1)) : new ArrayList<>();

    // get row groups of the last file to filter them or get all row groups if files metadata isn't available
    List<RowGroupMetadata> lastFileRowGroups = !qualifiedFiles.isEmpty() ? pruneRowGroupsForFiles(qualifiedFiles.subList(qualifiedFiles.size() - 1, qualifiedFiles.size())) : getRowGroupsMetadata();

    qualifiedRowGroups.addAll(limitMetadata(lastFileRowGroups, maxRecords));

    if (getRowGroupsMetadata() != null && getRowGroupsMetadata().size() == qualifiedRowGroups.size()) {
      logger.debug("limit push down does not apply, since number of row groups was not reduced.");
      return null;
    }

    return builder
        .withRowGroups(qualifiedRowGroups)
        .build();
  }
  // limit push down methods end

  // helper method used for partition pruning and filter push down
  @Override
  public void modifyFileSelection(FileSelection selection) {
    super.modifyFileSelection(selection);

    List<String> files = selection.getFiles();
    fileSet = new HashSet<>(files);
    entries = new ArrayList<>(files.size());

    entries.addAll(files.stream()
        .map(ReadEntryWithPath::new)
        .collect(Collectors.toList()));

    if (getRowGroupsMetadata() != null) {
      rowGroups = getRowGroupsMetadata().stream()
          .filter(entry -> fileSet.contains(entry.getLocation()))
          .collect(Collectors.toList());
    }

    tableMetadata = ParquetTableMetadataUtils.updateRowCount(getTableMetadata(), getRowGroupsMetadata());

    if (getFilesMetadata() != null) {
      this.files = getFilesMetadata().stream()
          .filter(entry -> fileSet.contains(entry.getLocation()))
          .collect(Collectors.toList());
    }

    if (getPartitionsMetadata() != null) {
      partitions = new ArrayList<>();
      for (PartitionMetadata entry : getPartitionsMetadata()) {
        for (String partLocation : entry.getLocations()) {
          if (fileSet.contains(partLocation)) {
            partitions.add(entry);
            break;
          }
        }
      }
    }
    rowGroupInfos = null;
  }

  // protected methods block
  @Override
  protected void init() throws IOException {
    super.init();

    this.partitionColumns = metadataProvider.getPartitionColumns();
    this.endpointAffinities = AffinityCreator.getAffinityMap(getRowGroupInfos());
  }

  protected List<RowGroupMetadata> getRowGroupsMetadata() {
    if (rowGroups == null) {
      rowGroups = ((ParquetMetadataProvider) metadataProvider).getRowGroupsMeta();
    }
    return rowGroups;
  }

  // abstract methods block start
  protected abstract Collection<CoordinationProtos.DrillbitEndpoint> getDrillbits();
  protected abstract AbstractParquetGroupScan cloneWithFileSelection(Collection<String> filePaths) throws IOException;
  // abstract methods block end

  /**
   * This class is responsible for filtering different metadata levels including row group level.
   */
  protected abstract static class RowGroupScanFilterer extends GroupScanWithMetadataFilterer {
    protected List<RowGroupMetadata> rowGroups;

    public RowGroupScanFilterer(AbstractGroupScanWithMetadata source) {
      super(source);
    }

    public RowGroupScanFilterer withRowGroups(List<RowGroupMetadata> rowGroups) {
      this.rowGroups = rowGroups;
      return this;
    }

    /**
     * Returns new {@link AbstractParquetGroupScan} instance to be populated with filtered metadata
     * from this {@link RowGroupScanFilterer} instance.
     *
     * @return new {@link AbstractParquetGroupScan} instance
     */
    protected abstract AbstractParquetGroupScan getNewScan();

    public List<RowGroupMetadata> getRowGroups() {
      return rowGroups;
    }

    @Override
    public AbstractParquetGroupScan build() {
      AbstractParquetGroupScan newScan = getNewScan();
      newScan.tableMetadata = tableMetadata;
      // updates common row count and nulls counts for every column
      if (newScan.getTableMetadata() != null && rowGroups != null && newScan.getRowGroupsMetadata().size() != rowGroups.size()) {
        newScan.tableMetadata = ParquetTableMetadataUtils.updateRowCount(newScan.getTableMetadata(), rowGroups);
      }
      newScan.partitions = partitions != null ? partitions : Collections.emptyList();
      newScan.files = files != null ? files : Collections.emptyList();
      newScan.rowGroups = rowGroups != null ? rowGroups : Collections.emptyList();
      newScan.matchAllMetadata = matchAllMetadata;
      // since builder is used when pruning happens, entries and fileSet should be expanded
      if (!newScan.getFilesMetadata().isEmpty()) {
        newScan.entries = newScan.getFilesMetadata().stream()
            .map(file -> new ReadEntryWithPath(file.getLocation()))
            .collect(Collectors.toList());

        newScan.fileSet = newScan.getFilesMetadata().stream()
            .map(LocationProvider::getLocation)
            .collect(Collectors.toSet());
      } else if (!newScan.getRowGroupsMetadata().isEmpty()) {
        newScan.entries = newScan.getRowGroupsMetadata().stream()
            .map(RowGroupMetadata::getLocation)
            .distinct()
            .map(ReadEntryWithPath::new)
            .collect(Collectors.toList());

        newScan.fileSet = newScan.getRowGroupsMetadata().stream()
          .map(LocationProvider::getLocation)
          .collect(Collectors.toSet());
      }

      newScan.endpointAffinities = AffinityCreator.getAffinityMap(newScan.getRowGroupInfos());

      return newScan;
    }

    @Override
    protected RowGroupScanFilterer getFiltered(OptionManager optionManager, FilterPredicate filterPredicate, Set<SchemaPath> schemaPathsInExpr) {
      super.getFiltered(optionManager, filterPredicate, schemaPathsInExpr);

      if (((AbstractParquetGroupScan) source).getRowGroupsMetadata() != null) {
        filterRowGroupMetadata(optionManager, filterPredicate, schemaPathsInExpr);
      }
      return this;
    }

    /**
     * Produces filtering of metadata at row group level.
     *
     * @param optionManager     option manager
     * @param filterPredicate   filter expression
     * @param schemaPathsInExpr columns used in filter expression
     */
    protected void filterRowGroupMetadata(OptionManager optionManager,
                                          FilterPredicate filterPredicate,
                                          Set<SchemaPath> schemaPathsInExpr) {
      AbstractParquetGroupScan abstractParquetGroupScan = (AbstractParquetGroupScan) source;
      List<RowGroupMetadata> prunedRowGroups;
      if (CollectionUtils.isNotEmpty(abstractParquetGroupScan.getFilesMetadata())
          && abstractParquetGroupScan.getFilesMetadata().size() > getFiles().size()) {
        // prunes row groups to leave only row groups which are contained by pruned files
        prunedRowGroups = abstractParquetGroupScan.pruneRowGroupsForFiles(getFiles());
      } else if (CollectionUtils.isNotEmpty(abstractParquetGroupScan.getPartitionsMetadata())
          && abstractParquetGroupScan.getPartitionsMetadata().size() > getPartitions().size()) {
        // prunes row groups to leave only row groups which are contained by pruned partitions
        prunedRowGroups = pruneForPartitions(abstractParquetGroupScan.getRowGroupsMetadata(), getPartitions());
      } else {
        // no partition or file pruning happened, no need to prune initial row groups list
        prunedRowGroups = abstractParquetGroupScan.getRowGroupsMetadata();
      }

      if (isMatchAllMetadata()) {
        rowGroups = prunedRowGroups;
        return;
      }

      // Stop files pruning for the case:
      //    -  # of row groups is beyond PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD.
      if (prunedRowGroups.size() <= optionManager.getOption(
        PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD)) {
        matchAllMetadata = true;
        this.rowGroups = filterAndGetMetadata(schemaPathsInExpr, prunedRowGroups, filterPredicate, optionManager);
      } else {
        this.rowGroups = prunedRowGroups;
        matchAllMetadata = false;
        overflowLevel = MetadataLevel.ROW_GROUP;
      }
    }
  }

}
