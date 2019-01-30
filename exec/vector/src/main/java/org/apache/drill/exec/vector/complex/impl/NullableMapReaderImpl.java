/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.drill.exec.vector.complex.impl;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.complex.NullableMapVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

public class NullableMapReaderImpl extends AbstractFieldReader {
  private final NullableMapVector vector;

  public NullableMapReaderImpl(NullableMapVector vector) {
    super();
    this.vector = vector;
  }

  @Override
  public FieldReader reader(String name) {
    return vector.getValuesVector().getReader().reader(name);
  }

  @Override
  public MaterializedField getField() {
    return vector.getField();
  }

  @Override
  public Object readObject() {
    return vector.getAccessor().getObject(idx());
  }

  @Override
  public boolean isSet() {
    return vector.getAccessor().isSet(idx());
  }

  @Override
  public TypeProtos.MajorType getType() {
    return vector.getField().getType();
  }

  @Override
  public java.util.Iterator<String> iterator() {
    return vector.fieldNameIterator();
  }

  @Override
  public void copyAsValue(BaseWriter.MapWriter writer) {
    NullableMapWriter impl = (NullableMapWriter) writer;
    impl.container.copyFromSafe(idx(), impl.idx(), vector);
  }

  @Override
  public void copyAsField(String name, BaseWriter.MapWriter writer) {
    NullableMapWriter impl = (NullableMapWriter) writer.map(name);
    impl.container.copyFromSafe(idx(), impl.idx(), vector);
  }
}
