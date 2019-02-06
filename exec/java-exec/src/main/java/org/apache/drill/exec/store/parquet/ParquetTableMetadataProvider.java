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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.TableMetadataProvider;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.metastore.TableMetadata;

import java.util.List;

public class ParquetTableMetadataProvider implements TableMetadataProvider {
  private BaseTableMetadataCreator metadataCreator;

  public ParquetTableMetadataProvider(BaseTableMetadataCreator metadataCreator) {
    this.metadataCreator = metadataCreator;
  }

  @Override
  public TableMetadata getTableMetadata(String location, String tableName) {
    return metadataCreator.getTableMetadata();
  }

  @Override
  public List<PartitionMetadata> getPartitionsMetadata(String location, String tableName) {
    return metadataCreator.getPartitionMetadata();
  }

  @Override
  public PartitionMetadata getPartitionMetadata(String location, String tableName, String columnName) {
//    return metadataCreator.getPartitionMetadata().get(columnName); // TODO: introduce Map<String, PartitionMetadata> in the metadataCreator, where String is the columnName
    return null;
  }

  @Override
  public FileMetadata getFileMetadata(String location, String tableName) {
    return null;
  }

  @Override
  public List<FileMetadata> getFilesForPartition(PartitionMetadata partition) {
    return null;
  }

  @Override
  public List<FileMetadata> getFiles(String location, String tableName) {
    return metadataCreator.getFilesMetadata();
  }

  @Override
  public String getSelectionRoot() {
    return metadataCreator.getSelectionRoot();
  }

  public List<RowGroupMetadata> getRowGroupsMeta() {
    return metadataCreator.getRowGroupsMeta();
  }

  public boolean isUsedMetadataCache() {
    return metadataCreator.isUsedMetadataCache();
  }

  @Override
  public List<ReadEntryWithPath> getEntries() {
    return metadataCreator.getEntries();
  }

  @Override
  public List<SchemaPath> getPartitionColumns() {
    return metadataCreator.getPartitionColumns();
  }
}
