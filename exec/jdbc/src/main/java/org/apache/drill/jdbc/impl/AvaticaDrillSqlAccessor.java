/**
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
package org.apache.drill.jdbc.impl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.calcite.avatica.util.Cursor.Accessor;
import org.apache.drill.exec.vector.accessor.SqlAccessor;
import org.apache.drill.jdbc.InvalidCursorStateSqlException;


// TODO:  Revisit adding null check for non-primitive types to SqlAccessor's
// contract and classes generated by SqlAccessor template (DRILL-xxxx).

class AvaticaDrillSqlAccessor implements Accessor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvaticaDrillSqlAccessor.class);

  private final static byte PRIMITIVE_NUM_NULL_VALUE = 0;
  private final static boolean BOOLEAN_NULL_VALUE = false;

  private SqlAccessor underlyingAccessor;
  private DrillCursor cursor;

  AvaticaDrillSqlAccessor(SqlAccessor drillSqlAccessor, DrillCursor cursor) {
    super();
    this.underlyingAccessor = drillSqlAccessor;
    this.cursor = cursor;
  }

  private int getCurrentRecordNumber() throws SQLException {
    // WORKAROUND:  isBeforeFirst can't be called first here because AvaticaResultSet
    // .next() doesn't increment its row field when cursor.next() returns false,
    // so in that case row can be left at -1, so isBeforeFirst() returns true
    // even though we're not longer before the empty set of rows--and it's all
    // private, so we can't get to it to override any of several candidates.
    if ( cursor.isAfterLast() ) {
      throw new InvalidCursorStateSqlException(
          "Result set cursor is already positioned past all rows." );
    }
    else if ( cursor.isBeforeFirst() ) {
      throw new InvalidCursorStateSqlException(
          "Result set cursor is positioned before all rows.  Call next() first." );
    }
    else {
      return cursor.getCurrentRecordNumber();
    }
  }

  /**
   * @see SQLAccessor#getObjectClass()
   */
  public Class<?> getObjectClass() {
    return underlyingAccessor.getObjectClass();
  }

  @Override
  public boolean wasNull() throws SQLException {
    return underlyingAccessor.isNull(getCurrentRecordNumber());
  }

  @Override
  public String getString() throws SQLException {
    return underlyingAccessor.getString(getCurrentRecordNumber());
  }

  @Override
  public boolean getBoolean() throws SQLException {
    return underlyingAccessor.isNull(getCurrentRecordNumber())
        ? BOOLEAN_NULL_VALUE
        : underlyingAccessor.getBoolean(getCurrentRecordNumber());
  }

  @Override
  public byte getByte() throws SQLException {
    return underlyingAccessor.isNull(getCurrentRecordNumber())
        ? PRIMITIVE_NUM_NULL_VALUE
        : underlyingAccessor.getByte(getCurrentRecordNumber());
  }

  @Override
  public short getShort() throws SQLException {
    return underlyingAccessor.isNull(getCurrentRecordNumber())
        ? PRIMITIVE_NUM_NULL_VALUE
        : underlyingAccessor.getShort(getCurrentRecordNumber());
  }

  @Override
  public int getInt() throws SQLException {
    return underlyingAccessor.isNull(getCurrentRecordNumber())
        ? PRIMITIVE_NUM_NULL_VALUE
        : underlyingAccessor.getInt(getCurrentRecordNumber());
  }

  @Override
  public long getLong() throws SQLException {
    return underlyingAccessor.isNull(getCurrentRecordNumber())
        ? PRIMITIVE_NUM_NULL_VALUE
        : underlyingAccessor.getLong(getCurrentRecordNumber());
  }

  @Override
  public float getFloat() throws SQLException {
    return underlyingAccessor.isNull(getCurrentRecordNumber())
        ? PRIMITIVE_NUM_NULL_VALUE
        : underlyingAccessor.getFloat(getCurrentRecordNumber());
  }

  @Override
  public double getDouble() throws SQLException {
    return underlyingAccessor.isNull(getCurrentRecordNumber())
        ? PRIMITIVE_NUM_NULL_VALUE
        : underlyingAccessor.getDouble(getCurrentRecordNumber());
  }

  @Override
  public BigDecimal getBigDecimal() throws SQLException {
    return underlyingAccessor.getBigDecimal(getCurrentRecordNumber());
  }

  @Override
  public BigDecimal getBigDecimal(int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public byte[] getBytes() throws SQLException {
    return underlyingAccessor.getBytes(getCurrentRecordNumber());
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    return underlyingAccessor.getStream(getCurrentRecordNumber());
  }

  @Override
  public InputStream getUnicodeStream() throws SQLException {
    return underlyingAccessor.getStream(getCurrentRecordNumber());
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    return underlyingAccessor.getStream(getCurrentRecordNumber());
  }

  @Override
  public Object getObject() throws SQLException {
    return underlyingAccessor.getObject(getCurrentRecordNumber());
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    return underlyingAccessor.getReader(getCurrentRecordNumber());
  }

  @Override
  public Object getObject(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(Calendar calendar) throws SQLException {
    return underlyingAccessor.getDate(getCurrentRecordNumber());
  }

  @Override
  public Time getTime(Calendar calendar) throws SQLException {
    return underlyingAccessor.getTime(getCurrentRecordNumber());
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) throws SQLException {
    return underlyingAccessor.getTimestamp(getCurrentRecordNumber());
  }

  @Override
  public URL getURL() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString() throws SQLException {
    return underlyingAccessor.getString(getCurrentRecordNumber());
  }

  @Override
  public Reader getNCharacterStream() throws SQLException {
    return underlyingAccessor.getReader(getCurrentRecordNumber());
  }

  @Override
  public <T> T getObject(Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

}