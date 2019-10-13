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
package org.apache.drill.exec.metastore;

import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.record.metadata.schema.SchemaProvider;
import org.apache.drill.metastore.MetastoreRegistry;
import org.apache.drill.metastore.metadata.TableInfo;
import org.apache.drill.metastore.metadata.TableMetadataProvider;
import org.apache.drill.metastore.metadata.TableMetadataProviderBuilder;

public class MetastoreMetadataProviderManager implements MetadataProviderManager {

  private final MetastoreRegistry metastoreRegistry;
  private final TableInfo tableInfo;

  private TableMetadataProvider tableMetadataProvider;

  private SchemaProvider schemaProvider;
  private DrillStatsTable statsProvider;

  public MetastoreMetadataProviderManager(MetastoreRegistry metastoreRegistry, TableInfo tableInfo) {
    this.metastoreRegistry = metastoreRegistry;
    this.tableInfo = tableInfo;
  }

  @Override
  public void setSchemaProvider(SchemaProvider schemaProvider) {
    this.schemaProvider = schemaProvider;
  }

  @Override
  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  @Override
  public void setStatsProvider(DrillStatsTable statsProvider) {
    this.statsProvider = statsProvider;
  }

  @Override
  public DrillStatsTable getStatsProvider() {
    return statsProvider;
  }

  @Override
  public void setTableMetadataProvider(TableMetadataProvider tableMetadataProvider) {
    this.tableMetadataProvider = tableMetadataProvider;
  }

  @Override
  public TableMetadataProvider getTableMetadataProvider() {
    return tableMetadataProvider;
  }

  public MetastoreRegistry getMetastoreRegistry() {
    return metastoreRegistry;
  }

  public TableInfo getTableInfo() {
    return tableInfo;
  }

  @Override
  public TableMetadataProviderBuilder builder(MetadataProviderKind kind) {
    switch (kind) {
      case PARQUET_TABLE:
        return new MetastoreParquetTableMetadataProvider.Builder(this);
    }
    return null;
  }
}
