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
package org.apache.drill.exec.physical.impl.metadata;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MetadataHandlerPOP;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.metastore.components.tables.BasicTablesRequests;
import org.apache.drill.metastore.components.tables.Tables;
import org.apache.drill.metastore.metadata.BaseMetadata;
import org.apache.drill.metastore.metadata.MetadataType;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MetadataHandlerBatch extends AbstractSingleRecordBatch<MetadataHandlerPOP> {
  private static final Logger logger = LoggerFactory.getLogger(MetadataHandlerBatch.class);

  private final Tables tables;
  private List<String> locations;
  private MetadataType metadataType;
  private String metadataKey;
  private Configuration fsConf;
  private FileSystem fileSystem;
  private boolean firstBatch = true;
  private boolean metadataSent = false;
  private List<String> modifiedPaths;

  protected MetadataHandlerBatch(MetadataHandlerPOP popConfig,
      FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, incoming);
    this.tables = context.getMetastore().tables();
    this.metadataType = popConfig.getMetadataType();
    try {
      fsConf = new Configuration();
      this.fileSystem = FileSystem.get(fsConf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public IterOutcome doWork() {
    // 1. Consume data from incoming operators and store metadata keys for which it was returned
    // 2. For metadata parts whose metadata wasn't returned by incoming operator,
    // compare its last modified time with the last modified time from the metastore
    // 2.1 For the case when incoming operator returned nothing - no updated underlying metadata was found.
    // 2.2 Discovers metadata parts which were removed (comparing last modified times)
    // 2.3 For the case when something was removed, reads metadata for unchanged data
    // (changed was already sent from metastore) and stores it into the container.
    // 2.4 If nothing was removed and nothing was returned by incoming operators, return empty results.

    BasicTablesRequests basicTablesRequests = tables.basicRequests();
    // TODO: temporary replaced with NE, revert
    if (modifiedPaths != null) {
      Map<String, Long> lastModifiedTimes;
      switch (metadataType) {
        case ROW_GROUP:
        case FILE:
          lastModifiedTimes = basicTablesRequests.filesLastModifiedTime(popConfig.getTableInfo(), metadataKey, locations);
          break;
        case SEGMENT:
          lastModifiedTimes = basicTablesRequests.segmentsLastModifiedTime(popConfig.getTableInfo(), locations);
          break;
        default:
          lastModifiedTimes = Collections.emptyMap();
      }

      modifiedPaths = new ArrayList<>();
      lastModifiedTimes.forEach((path, lastModifiedTime) -> {
        if (isChanged(path, lastModifiedTime)) {
          modifiedPaths.add(path);
        }
      });
    }

    IterOutcome outcome;
    outcome = next(incoming);

    switch (outcome) {
      case NONE:
        if (firstBatch) {
          Preconditions.checkState(modifiedPaths.isEmpty(),
              "incoming batch didn't return the result for modified segments");
          // underlying data wasn't changed, return NONE to signalize that
          // metadata shouldn't be updated for current segment
          return outcome;
        } else if (!modifiedPaths.isEmpty()) {
          // end reading incoming metadata, read unchanged metadata from metastore
          // TODO: add logic to read metadata for unmodified paths from metastore and store it into the container

          modifiedPaths.clear();
          return IterOutcome.OK;
        }
        return outcome;
      case OK_NEW_SCHEMA:
        if (firstBatch) {
          firstBatch = false;
          if (!setupNewSchema()) {
            outcome = IterOutcome.OK;
          }
        }
        doWorkInternal();
        return outcome;
      case OK:
        assert !firstBatch : "First batch should be OK_NEW_SCHEMA";
        doWorkInternal();
      case OUT_OF_MEMORY:
      case NOT_YET:
      case STOP:
        return outcome;
      default:
        throw new UnsupportedOperationException("Unsupported upstream state " + outcome);
    }

//    boolean didSomeWork = false;
//    // obtain all incoming data to decide whether we should read data from the metastore
//    try {
//      outer:
//      while (true) {
//        outcome = next(incoming);
//        switch (outcome) {
//          case NONE:
//            break outer;
//          case OUT_OF_MEMORY:
//          case NOT_YET:
//          case STOP:
//            return outcome;
//          case OK_NEW_SCHEMA:
//            if (first) {
//              first = false;
//              if (!setupNewSchema()) {
//                outcome = IterOutcome.OK;
//              }
//              return outcome;
//            }
//            //fall through
//          case OK:
//            assert first == false : "First batch should be OK_NEW_SCHEMA";
//            IterOutcome out = doWork();
//            didSomeWork = true;
//            if (out != IterOutcome.OK) {
//              return out;
//            }
//            break;
//          default:
//            throw new UnsupportedOperationException("Unsupported upstream state " + outcome);
//        }
//      }
//    } catch (SchemaChangeException ex) {
//      kill(false);
//      context.getExecutorState().fail(UserException.unsupportedError(ex).build(logger));
//      return IterOutcome.STOP;
//    }
//
//    // We can only get here if upstream is NONE i.e. no more batches. If we did some work prior to
//    // exhausting all upstream, then return OK. Otherwise, return NONE.
//    if (didSomeWork) {
//      IterOutcome out = buildOutgoingRecordBatch();
//      finished = true;
//      return out;
//    } else {
//      return outcome;
//    }


//    incoming.getContext().
  }

  protected boolean setupNewSchema() {
    // TODO: add logic for constructing schema
    container.clear();

    for (VectorWrapper<?> vectorWrapper : incoming) {
      if (vectorWrapper.getField().getType().getMinorType() != TypeProtos.MinorType.NULL) {
        container.addOrGet(vectorWrapper.getField());
      }
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);

//    // Generate the list of fields for which statistics will be merged
//    buildColumnsList();
//    // Generate the schema for the outgoing record batch
//    buildOutputContainer();
    return true;
  }

//  protected void buildSchema() throws SchemaChangeException {
//    container.clear();
//    BatchSchema schema = incoming.getSchema();
//    for (MaterializedField materializedField : schema) {
//      // TODO: investigate either pass real callback
//      container.addOrGet(materializedField, null);
//    }
//
////    TypeProtos.MajorType mapType = TypeProtos.MajorType.newBuilder()
////        .setMinorType(TypeProtos.MinorType.MAP)
////        .setMode(TypeProtos.DataMode.REQUIRED)
////        .build();
////    container.addOrGet("map_field",
////        mapType, MapVector.class);
//    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
//
//  }

  private void buildColumnsList() {

  }

  private <T extends BaseMetadata> void handleMetastoreMetadata() {
    // TODO: should we also read underlying metadata here?
    List<String> metadataToRead = new ArrayList<>(locations);
    metadataToRead.removeAll(modifiedPaths);
    BasicTablesRequests basicTablesRequests = tables.basicRequests();
    List<? extends BaseMetadata> metadata;
    switch (metadataType) {
      case ROW_GROUP:
        metadata = basicTablesRequests.rowGroupsMetadata(popConfig.getTableInfo(), metadataKey, locations);
        break;
      case FILE:
        metadata = basicTablesRequests.filesMetadata(popConfig.getTableInfo(), metadataKey, locations);
        break;
      case SEGMENT:
        metadata = basicTablesRequests.segmentsMetadataByMetadataKey(popConfig.getTableInfo(), locations, metadataKey);
        break;
      case TABLE:
        metadata = Collections.singletonList(basicTablesRequests.tableMetadata(popConfig.getTableInfo()));
        break;
      default:
        throw new UnsupportedOperationException("Unsupported metadata type");
    }


  }

  // TODO: generalize logic to extend functionality not oly for file system tables
  private boolean isChanged(String path, Long lastModifiedTime) {
    try {
      long modificationTime = fileSystem.getFileStatus(new Path(path)).getModificationTime();
      return lastModifiedTime != null && lastModifiedTime == modificationTime;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected IterOutcome doWorkInternal() {
    container.transferIn(incoming.getContainer());
    container.setRecordCount(incoming.getRecordCount());
    VarCharVector valueVector = container.addOrGet(
        MaterializedField.create(MetadataAggBatch.METADATA_TYPE, Types.required(TypeProtos.MinorType.VARCHAR)));
    valueVector.allocateNew();
    // TODO: replace with adequate solution
    for (int i = 0; i < incoming.getRecordCount(); i++) {
      valueVector.getMutator().setSafe(i, metadataType.name().getBytes());
    }

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return IterOutcome.OK;
  }

  @Override
  public void dump() {
    logger.error("MetadataHandlerBatch[container={}, popConfig={}]", container, popConfig);
  }

  @Override
  public int getRecordCount() {
    return container.getRecordCount();
  }
}
