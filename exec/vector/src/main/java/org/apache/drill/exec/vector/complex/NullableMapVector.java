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
package org.apache.drill.exec.vector.complex;

import com.google.common.collect.ObjectArrays;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.NullableMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class NullableMapVector extends AbstractMapVector implements NullableVector {

  public final static TypeProtos.MajorType TYPE = Types.optional(TypeProtos.MinorType.MAP);

  private final MaterializedField bitsField = MaterializedField.create("$bits$", Types.required(TypeProtos.MinorType.MAP));
  private final UInt1Vector bits = new UInt1Vector(bitsField, allocator);
  private final MapVector values = new MapVector(field, allocator, callBack);

  private final NullableMapReaderImpl reader = new NullableMapReaderImpl(NullableMapVector.this);
  private final NullableMapVector.Accessor accessor = new NullableMapVector.Accessor();
  private final NullableMapVector.Mutator mutator = new NullableMapVector.Mutator();

  public NullableMapVector(String path, BufferAllocator allocator, CallBack callBack) {
    this(MaterializedField.create(path, TYPE), allocator, callBack);
  }

  public NullableMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    super(field, allocator, callBack);
    MaterializedField clonedField = field.clone();
    // create the hierarchy of the child vectors based on the materialized field
    for (MaterializedField child : clonedField.getChildren()) {
      if (!child.equals(BaseRepeatedValueVector.OFFSETS_FIELD)) {
        final String fieldName = child.getLastName();
        final ValueVector v = BasicTypeHelper.getNewVector(child, allocator, callBack);
        values.putVector(fieldName, v);
      }
    }
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  public void copyFromSafe(int fromIndex, int thisIndex, NullableMapVector from) {
    values.copyFromSafe(fromIndex, thisIndex, (MapVector) from.getValuesVector());
    bits.getMutator().setSafe(thisIndex, 1);
  }

  @Override
  protected boolean supportsDirectRead() {
    return true;
  }

  public Iterator<String> fieldNameIterator() {
    return values.fieldNameIterator();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
    values.setInitialCapacity(numRecords);
  }

  @Override
  public int getBufferSize() {
    return values.getBufferSize() + bits.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    return values.getBufferSizeFor(valueCount) + bits.getBufferSizeFor(valueCount);
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    final DrillBuf[] buffers = ObjectArrays.concat(bits.getBuffers(false), values.getBuffers(false), DrillBuf.class);
    if (clear) {
      for (final DrillBuf buffer : buffers) {
        buffer.retain(1);
      }
      clear();
    }
    return buffers;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new MapTransferImpl(getField().getPath(), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new MapTransferImpl((NullableMapVector) to);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new MapTransferImpl(ref, allocator);
  }

  @Override
  public ValueVector getValuesVector() {
    return values;
  }

  @Override
  public int getValueCapacity() {
    return values.getValueCapacity();
  }

  @Override
  public NullableMapVector.Accessor getAccessor() {
    return accessor;
  }

  @Override
  public void load(UserBitShared.SerializedField metadata, DrillBuf buf) {
    clear();
    final UserBitShared.SerializedField bitsField = metadata.getChild(0);
    bits.load(bitsField, buf);

    final int capacity = buf.capacity();
    final int bitsLength = bitsField.getBufferLength();
    final UserBitShared.SerializedField valuesField = metadata.getChild(1);
    values.load(valuesField, buf.slice(bitsLength, capacity - bitsLength));
  }

  @Override
  public UserBitShared.SerializedField getMetadata() {
    return getField().getAsBuilder()
      .setBufferLength(getBufferSize())
      .setValueCount(values.getAccessor().getValueCount())
      .addChild(bits.getMetadata())
      .addChild(values.getMetadata())
      .build();
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return values.iterator();
  }

  @Override
  public NullableMapVector.Mutator getMutator() {
    return mutator;
  }

  public ValueVector getVectorById(int id) {
    return values.getVectorById(id);
  }

  @Override
  public void clear() {
    bits.clear();
    values.clear();
  }

  @Override
  public void close() {
    bits.close();
    values.close();
    super.close();
  }

  protected Collection<ValueVector> getChildren() {
    return values.getChildren();
  }

  /**
   * Returns a {@link org.apache.drill.exec.vector.ValueVector} corresponding to the given ordinal identifier.
   */
  public ValueVector getChildByOrdinal(int id) {
    return values.getChildByOrdinal(id);
  }

  /**
   * Returns a {@link org.apache.drill.exec.vector.ValueVector} instance of subtype of <T> corresponding to the given
   * field name if exists or null.
   */
  @Override
  public <T extends ValueVector> T getChild(String name, Class<T> clazz) {
    return values.getChild(name, clazz);
  }

  /**
   * Inserts the input vector into the map if it does not exist, replaces if it exists already
   * @param name  field name
   * @param vector  vector to be inserted
   */
  @Override
  protected void putVector(String name, ValueVector vector) {
    if (values != null) {
      values.putVector(name, vector);
    }
  }

  /**
   * Returns the number of underlying child vectors.
   */
  @Override
  public int size() {
    return values.size();
  }

  /**
   * Returns a list of scalar child vectors recursing the entire vector hierarchy.
   */
  public List<ValueVector> getPrimitiveVectors() {
    return values.getPrimitiveVectors();
  }

  /**
   * Returns a vector with its corresponding ordinal mapping if field exists or null.
   */
  @Override
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) {
    return values.getChildVectorWithOrdinal(name);
  }

  /**
   * Inserts the vector with the given name if it does not exist else replaces it with the new value.
   *
   * Note that this method does not enforce any vector type check nor throws a schema change exception.
   */
  protected void putChild(String name, ValueVector vector) {
    values.putChild(name, vector);
  }

  /**
   * Adds a new field with the given parameters or replaces the existing one and consequently returns the resultant
   * {@link org.apache.drill.exec.vector.ValueVector}.
   *
   * Execution takes place in the following order:
   * <ul>
   *   <li>
   *     if field is new, create and insert a new vector of desired type.
   *   </li>
   *   <li>
   *     if field exists and existing vector is of desired vector type, return the vector.
   *   </li>
   *   <li>
   *     if field exists and null filled, clear the existing vector; create and insert a new vector of desired type.
   *   </li>
   *   <li>
   *     otherwise, throw an {@link java.lang.IllegalStateException}
   *   </li>
   * </ul>
   *
   * @param name name of the field
   * @param type type of the field
   * @param clazz class of expected vector type
   * @param <T> class type of expected vector type
   * @throws java.lang.IllegalStateException raised if there is a hard schema change
   *
   * @return resultant {@link org.apache.drill.exec.vector.ValueVector}
   */
  @Override
  public <T extends ValueVector> T addOrGet(String name, TypeProtos.MajorType type, Class<T> clazz) {
//    if (values.size() == 0) {
      bits.getMutator().set(0, 1);
//    }
    return values.addOrGet(name, type, clazz);
  }

  @Override
  public MaterializedField getField() {
    return values.getField();
  }

  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      success = values.allocateNewSafe() && bits.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    bits.zeroVector();
    mutator.reset();
    accessor.reset();
    return success;
  }

  private class MapTransferImpl implements TransferPair {
    private final NullableMapVector to;
    private final TransferPair vectorTransferPair;

    public MapTransferImpl(String path, BufferAllocator allocator) {
      this(new NullableMapVector(MaterializedField.create(path, TYPE), allocator, new SchemaChangeCallBack()));
    }

    public MapTransferImpl(NullableMapVector to) {
      this.to = to;
      vectorTransferPair = values.makeTransferPair(to.values);
    }

    @Override
    public void transfer() {
      to.clear();
      bits.transferTo(to.bits);
      values.makeTransferPair(to.values).transfer();
      clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFromSafe(from, to, NullableMapVector.this);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      bits.splitAndTransferTo(startIndex, length, to.bits);
      vectorTransferPair.splitAndTransfer(startIndex, length);
      to.getMutator().setValueCount(length);
    }
  }

  public class Accessor extends BaseValueVector.BaseAccessor {
    final UInt1Vector.Accessor bAccessor = bits.getAccessor();

    final MapVector.Accessor vAccessor = values.getAccessor();

    @Override
    public boolean isNull(int index) {
      return isSet(index) == 0;
    }

    public int isSet(int index) {
      return bAccessor.get(index);
    }

    @Override
    public Object getObject(int index) {
      if (isNull(index)) {
        return null;
      } else {
        return vAccessor.getObject(index);
      }
    }

    public void get(int index, ComplexHolder holder) {
      vAccessor.get(index, holder);
      holder.isSet = bAccessor.get(index);
    }

    @Override
    public int getValueCount() {
      return values.getAccessor().getValueCount();
    }

    public void reset(){}
  }

  public class Mutator extends BaseValueVector.BaseMutator {


    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      values.getMutator().setValueCount(valueCount);
      bits.getMutator().setValueCount(valueCount);
      if (values.size() >= 0) {
        for (int i = 0; i <= valueCount; i++) {
          bits.getMutator().set(i, 1);
        }
      }
    }

    @Override
    public void generateTestData(int valueCount) {
      bits.getMutator().generateTestDataAlt(valueCount);
      values.getMutator().generateTestData(valueCount);

      setValueCount(valueCount);
    }
  }

  public void initialize() {
    bits.getMutator().set(0, 1);
  }
}
