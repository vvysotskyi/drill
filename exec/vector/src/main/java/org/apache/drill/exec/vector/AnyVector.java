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
package org.apache.drill.exec.vector;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

public class AnyVector extends BaseValueVector implements NullableVector {

  public final static TypeProtos.MajorType TYPE = Types.optional(TypeProtos.MinorType.LATE);

  private final MaterializedField bitsField = MaterializedField.create("$bits$", Types.required(TypeProtos.MinorType.UINT1));
  // Collects fields that were assigned by nulls
  private final UInt1Vector bits = new UInt1Vector(bitsField, allocator);

  public UInt1Vector getBits() {
    return bits;
  }

  private final Mutator mutator = new Mutator();
  private final Accessor accessor = new Accessor();

  public AnyVector(MaterializedField field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    if(!allocateNewSafe()){
      throw new OutOfMemoryException("Failure while allocating buffer.");
    }
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
      success = bits.allocateNewSafe();
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

  @Override
  public void setInitialCapacity(int numRecords) {
    bits.setInitialCapacity(numRecords);
  }

  @Override
  public int getValueCapacity() {
    return bits.getValueCapacity();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(getField(), allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(getField().withPath(ref), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((AnyVector) target);
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public FieldReader getReader() {
    return NullReader.INSTANCE;
  }

  @Override
  public int getBufferSize() {
    return bits.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    return bits.getBufferSizeFor(valueCount);
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    DrillBuf[] buffers = bits.getBuffers(false);
    if (clear) {
      for (DrillBuf buffer : buffers) {
        buffer.retain(1);
      }
      clear();
    }
    return buffers;
  }

  @Override
  public void load(UserBitShared.SerializedField metadata, DrillBuf buffer) {
    clear();
    final UserBitShared.SerializedField bitsField = metadata.getChild(0);
    bits.load(bitsField, buffer);
  }

  @Override
  public UserBitShared.SerializedField.Builder getMetadataBuilder() {
    return super.getMetadataBuilder()
      .addChild(bits.getMetadata());
  }

  public void copyFromSafe(int fromIndex, int thisIndex, AnyVector from) {
    bits.copyFromSafe(fromIndex, thisIndex, from.bits);
  }

  public void transferTo(AnyVector target) {
    bits.transferTo(target.bits);
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, AnyVector target) {
    bits.splitAndTransferTo(startIndex, length, target.bits);
  }


  @Override
  public void clear() {
    super.clear();
    bits.clear();
  }

  @Override
  public void close() {
    bits.close();
    super.close();
  }

  @Override
  public ValueVector getValuesVector() {
    return null;
  }

  private class TransferImpl implements TransferPair {
    AnyVector to;

    public TransferImpl(MaterializedField field, BufferAllocator allocator){
      to = new AnyVector(field, allocator);
    }

    public TransferImpl(AnyVector to){
      this.to = to;
    }

    @Override
    public AnyVector getTo(){
      return to;
    }

    @Override
    public void transfer(){
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, AnyVector.this);
    }
  }

  public final class Accessor extends BaseValueVector.BaseAccessor {
    final UInt1Vector.Accessor bAccessor = bits.getAccessor();

    @Override
    public boolean isNull(int index) {
      return bAccessor.get(index) == 2;
    }

    public boolean isSet(int index) {
      return bAccessor.get(index) == 1 || bAccessor.get(index) == 3;
    }

    @Override
    public Object getObject(int index) {
      if (bits.getAccessor().getObject(index) == 3) {
        return new JsonStringArrayList<>();
      }
      return null;
    }

    @Override
    public int getValueCount() {
      return bAccessor.getValueCount();
    }

    public void reset() {

    }
  }

  public final class Mutator extends BaseValueVector.BaseMutator {
    @Override
    public void setValueCount(int valueCount) {
      assert valueCount >= 0;
      bits.getMutator().setValueCount(valueCount);
    }

    public void setValue(int index) {
      assert index >= 0;
      bits.getMutator().set(index, 2);
      bits.getMutator().setValueCount(index + 1);
    }

    public void setInitializedValue(int index) {
      assert index >= 0;
      bits.getMutator().set(index, 1);
      bits.getMutator().setValueCount(index + 1);
    }

    public void setInitializedList(int index) {
      assert index >= 0;
      bits.getMutator().set(index, 3);
      bits.getMutator().setValueCount(index + 1);
    }

    @Override
    public void generateTestData(int valueCount) {

    }
  }
}
