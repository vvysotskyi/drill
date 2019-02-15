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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.exec.physical.base.ParquetTableMetadataProvider;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.metastore.LocationProvider;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

@JsonTypeName("parquet-scan")
public class ParquetGroupScan extends AbstractParquetGroupScan {

  private final ParquetFormatPlugin formatPlugin;
  private final ParquetFormatConfig formatConfig;

  private boolean usedMetadataCache; // false by default
  // may change when filter push down / partition pruning is applied
  private String selectionRoot;
  private String cacheFileRoot;

  @JsonCreator
  public ParquetGroupScan(@JacksonInject StoragePluginRegistry engineRegistry,
                          @JsonProperty("userName") String userName,
                          @JsonProperty("entries") List<ReadEntryWithPath> entries,
                          @JsonProperty("storage") StoragePluginConfig storageConfig,
                          @JsonProperty("format") FormatPluginConfig formatConfig,
                          @JsonProperty("columns") List<SchemaPath> columns,
                          @JsonProperty("selectionRoot") String selectionRoot,
                          @JsonProperty("cacheFileRoot") String cacheFileRoot,
                          @JsonProperty("readerConfig") ParquetReaderConfig readerConfig,
                          @JsonProperty("filter") LogicalExpression filter) throws IOException, ExecutionSetupException {
    super(ImpersonationUtil.resolveUserName(userName), columns, entries, readerConfig, filter);
    Preconditions.checkNotNull(storageConfig);
    Preconditions.checkNotNull(formatConfig);

    this.cacheFileRoot = cacheFileRoot;
    this.formatPlugin =
        Preconditions.checkNotNull((ParquetFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, formatConfig));
    this.formatConfig = this.formatPlugin.getConfig();
    DrillFileSystem fs =
        ImpersonationUtil.createFileSystem(ImpersonationUtil.resolveUserName(userName), formatPlugin.getFsConf());

    this.metadataProvider = new ParquetTableMetadataProviderImpl(entries, selectionRoot, cacheFileRoot, null,
        readerConfig, fs, this.formatConfig.areCorruptDatesAutoCorrected());
    this.selectionRoot = metadataProvider.getSelectionRoot();
    this.tableMetadata = metadataProvider.getTableMetadata();
    this.partitions = metadataProvider.getPartitionsMetadata();
    this.files = metadataProvider.getFilesMetadata();
    this.fileSet = metadataProvider.getFileSet();
    this.partitionColumns = metadataProvider.getPartitionColumns();

    ParquetTableMetadataProvider metadataProvider = (ParquetTableMetadataProvider) this.metadataProvider;
    this.usedMetadataCache = metadataProvider.isUsedMetadataCache();
    this.entries = metadataProvider.getEntries();
    this.rowGroups = metadataProvider.getRowGroupsMeta();

    init();
  }

  // TODO: replace constructors by ParquetTableMetadataCreator usage and
  //  add method for creating ParquetGroupScan
  public ParquetGroupScan(String userName,
                          FileSelection selection,
                          ParquetFormatPlugin formatPlugin,
                          List<SchemaPath> columns,
                          ParquetReaderConfig readerConfig) throws IOException {
    this(userName, selection, formatPlugin, columns, readerConfig, ValueExpressions.BooleanExpression.TRUE);
  }

  public ParquetGroupScan(String userName,
                          FileSelection selection,
                          ParquetFormatPlugin formatPlugin,
                          List<SchemaPath> columns,
                          ParquetReaderConfig readerConfig,
                          LogicalExpression filter) throws IOException {
    super(userName, columns, new ArrayList<>(), readerConfig, filter);

    this.formatPlugin = formatPlugin;
    this.formatConfig = formatPlugin.getConfig();
    this.cacheFileRoot = selection.getCacheFileRoot();

    DrillFileSystem fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.resolveUserName(userName), formatPlugin.getFsConf());
    metadataProvider = new ParquetTableMetadataProviderImpl(selection, readerConfig, fs,
        formatConfig.areCorruptDatesAutoCorrected());
    this.selectionRoot = metadataProvider.getSelectionRoot();
    this.tableMetadata = metadataProvider.getTableMetadata();
    this.files = metadataProvider.getFilesMetadata();
    this.partitions = metadataProvider.getPartitionsMetadata();
    this.partitionColumns = metadataProvider.getPartitionColumns();
    this.fileSet = metadataProvider.getFileSet();

    ParquetTableMetadataProvider metadataProvider = (ParquetTableMetadataProvider) this.metadataProvider;
    this.usedMetadataCache = metadataProvider.isUsedMetadataCache();
    this.entries = metadataProvider.getEntries();
    this.rowGroups = metadataProvider.getRowGroupsMeta();


    // TODO: initialize TableMetadata, FileMetadata and RowGroupMetadata from
    //  parquetTableMetadata if it wasn't fetched from the metastore using ParquetTableMetadataCreator

    init();
  }

  /**
   * Copy constructor for shallow partial cloning
   * @param that old groupScan
   */
  private ParquetGroupScan(ParquetGroupScan that) {
    this(that, null);
  }

  /**
   * Copy constructor for shallow partial cloning with new {@link FileSelection}
   * @param that old groupScan
   * @param selection new selection
   */
  private ParquetGroupScan(ParquetGroupScan that, FileSelection selection) {
    super(that);
    this.formatConfig = that.formatConfig;
    this.formatPlugin = that.formatPlugin;
    this.selectionRoot = that.selectionRoot;
    this.cacheFileRoot = selection == null ? that.cacheFileRoot : selection.getCacheFileRoot();
    this.usedMetadataCache = that.usedMetadataCache;
    this.partitionColumns = that.partitionColumns;
  }

  // getters for serialization / deserialization start
  @JsonProperty("format")
  public ParquetFormatConfig getFormatConfig() {
    return formatConfig;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getEngineConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty
  public String getSelectionRoot() {
    return selectionRoot;
  }

  @JsonProperty
  public String getCacheFileRoot() {
    return cacheFileRoot;
  }
  // getters for serialization / deserialization end

  @Override
  public ParquetRowGroupScan getSpecificScan(int minorFragmentId) {
    return new ParquetRowGroupScan(getUserName(), formatPlugin, getReadEntries(minorFragmentId), columns, readerConfig, selectionRoot, filter);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetGroupScan(this);
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    ParquetGroupScan newScan = new ParquetGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public ParquetGroupScan clone(FileSelection selection) throws IOException {
    // TODO: rewrite in accordance to the new logic
    ParquetGroupScan newScan = new ParquetGroupScan(this, selection);
    newScan.modifyFileSelection(selection);
    newScan.init();
    return newScan;
  }

  private List<ReadEntryWithPath> entries() {
    return entries;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("ParquetGroupScan [");
    builder.append("entries=").append(entries());
    builder.append(", selectionRoot=").append(selectionRoot);
    // TODO: solve whether print entries when no pruning is done or list of files
    //  and the actual number instead of root and 1 file...
    builder.append(", numFiles=").append(getEntries().size());
    builder.append(", numRowGroups=").append(rowGroups.size());
    builder.append(", usedMetadataFile=").append(usedMetadataCache);

    String filterString = getFilterString();
    if (!filterString.isEmpty()) {
      builder.append(", filter=").append(filterString);
    }

    if (usedMetadataCache) {
      // For EXPLAIN, remove the URI prefix from cacheFileRoot.  If cacheFileRoot is null, we
      // would have read the cache file from selectionRoot
      String cacheFileRootString = (cacheFileRoot == null) ?
          Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString() :
          Path.getPathWithoutSchemeAndAuthority(new Path(cacheFileRoot)).toString();
      builder.append(", cacheFileRoot=").append(cacheFileRootString);
    }

    builder.append(", columns=").append(columns);
    builder.append("]");

    return builder.toString();
  }

  // overridden protected methods block start
  @Override
  protected AbstractParquetGroupScan cloneWithFileSelection(Collection<String> filePaths) throws IOException {
    FileSelection newSelection = new FileSelection(null, new ArrayList<>(filePaths), getSelectionRoot(), cacheFileRoot, false);
    return clone(newSelection);
  }

  @Override
  protected RowGroupScanBuilder getBuilder() {
    return new ParquetGroupScanBuilder(this);
  }

  @Override
  protected Collection<DrillbitEndpoint> getDrillbits() {
    return formatPlugin.getContext().getBits();
  }

  @Override
  protected boolean supportsFileImplicitColumns() {
    return selectionRoot != null;
  }

  @Override
  protected List<String> getPartitionValues(LocationProvider rowGroupInfo) {
    return ColumnExplorer.listPartitionValues(rowGroupInfo.getLocation(), selectionRoot);
  }
  // overridden protected methods block end

  private static class ParquetGroupScanBuilder extends RowGroupScanBuilder {

    public ParquetGroupScanBuilder(ParquetGroupScan source) {
      this.newScan = new ParquetGroupScan(source);
    }
  }
}
