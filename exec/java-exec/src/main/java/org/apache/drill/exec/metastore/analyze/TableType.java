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
package org.apache.drill.exec.metastore.analyze;

import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

/**
 * Enum of table types.
 */
public enum TableType {

  /**
   * Parquet table type
   */
  PARQUET(true),

  /**
   * CSV table type
   */
  CSV(true),

  /**
   * JSON table type
   */
  JSON(true);

  private final boolean isFileBased;

  TableType(boolean isFileBased) {
    this.isFileBased = isFileBased;
  }

  /**
   * Whether the table is file-based.
   *
   * @return true if the table is file-based.
   */
  public boolean isFileBased() {
    return isFileBased;
  }

  /**
   * Returns {@link TableType} which corresponds to specified {@link GroupScan}.
   *
   * @param groupScan group scan for which should be returned table type
   * @return {@link TableType}
   */
  public static TableType getTableType(GroupScan groupScan) {
    if (groupScan instanceof ParquetGroupScan) {
      return TableType.PARQUET;
    }
    throw new UnsupportedOperationException("Unsupported table type.");
  }
}
