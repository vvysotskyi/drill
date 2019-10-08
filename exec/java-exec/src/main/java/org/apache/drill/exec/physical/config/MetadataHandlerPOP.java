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
package org.apache.drill.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.metastore.metadata.MetadataInfo;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.metastore.metadata.TableInfo;

import java.util.List;

@JsonTypeName("metadataHandler")
public class MetadataHandlerPOP extends AbstractSingle {
  private final TableInfo tableInfo;
  private final List<MetadataInfo> metadataToHandle;
  private final MetadataType metadataType;
  private final int depthLevel;
  private final List<String> segmentColumns;
  private final List<String> locations;

  @JsonCreator
  public MetadataHandlerPOP(@JsonProperty("child") PhysicalOperator child,
      @JsonProperty("tableInfo") TableInfo tableInfo,
      @JsonProperty("metadataToHandle") List<MetadataInfo> metadataToHandle,
      @JsonProperty("metadataType") MetadataType metadataType,
      @JsonProperty("depthLevel") int depthLevel,
      @JsonProperty("segmentColumns") List<String> segmentColumns,
      @JsonProperty("locations") List<String> locations) {
    super(child);
    this.tableInfo = tableInfo;
    this.metadataToHandle = metadataToHandle;
    this.metadataType = metadataType;
    this.depthLevel = depthLevel;
    this.segmentColumns = segmentColumns;
    this.locations = locations;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new MetadataHandlerPOP(child, tableInfo, metadataToHandle, metadataType, depthLevel, segmentColumns, locations);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitOp(this, value);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.METADATA_HANDLER_VALUE;
  }

  public TableInfo getTableInfo() {
    return tableInfo;
  }

  public List<MetadataInfo> getMetadataToHandle() {
    return metadataToHandle;
  }

  public MetadataType getMetadataType() {
    return metadataType;
  }

  public int getDepthLevel() {
    return depthLevel;
  }

  public List<String> getSegmentColumns() {
    return segmentColumns;
  }
}
