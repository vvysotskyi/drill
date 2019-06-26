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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.RecordReader;

import java.util.List;
import java.util.Map;

// TODO: remove?
public class MetadataScanBatch extends ScanBatch {
  public MetadataScanBatch(FragmentContext context, OperatorContext oContext, List<? extends RecordReader> readerList, List<Map<String, String>> implicitColumnList) {
    super(context, oContext, readerList, implicitColumnList);
  }

  public MetadataScanBatch(PhysicalOperator subScanConfig, FragmentContext context, List<RecordReader> readers) throws ExecutionSetupException {
    super(subScanConfig, context, readers);
  }

  public MetadataScanBatch(PhysicalOperator subScanConfig, FragmentContext context, List<RecordReader> readerList, boolean isRepeatableScan) throws ExecutionSetupException {
    super(subScanConfig, context, readerList, isRepeatableScan);
  }
}
