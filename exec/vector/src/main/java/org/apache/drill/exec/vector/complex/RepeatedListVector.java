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
package org.apache.drill.exec.vector.complex;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.RepeatedListHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.AddOrGetResult;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.AnyVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorDescriptor;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.RepeatedListReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

public class RepeatedListVector extends AbstractContainerVector
    implements RepeatedValueVector, RepeatedFixedWidthVectorLike {

  public final static MajorType TYPE = Types.repeated(MinorType.LIST);
  private final RepeatedListReaderImpl reader = new RepeatedListReaderImpl(null, this);
  private final DelegateRepeatedVector delegate;

  protected static class DelegateRepeatedVector extends BaseRepeatedValueVector {

    private final RepeatedListAccessor accessor = new RepeatedListAccessor();
    private final RepeatedListMutator mutator = new RepeatedListMutator();
    private final EmptyValuePopulator emptyPopulator;
    private transient DelegateTransferPair ephPair;
    private final MaterializedField bitsField = MaterializedField.create("$bits$", Types.required(TypeProtos.MinorType.UINT1));
    private final UInt1Vector bits = new UInt1Vector(bitsField, allocator);

    public class RepeatedListAccessor extends BaseRepeatedValueVector.BaseRepeatedAccessor {

      @Override
      public Object getObject(int index) {
        if (isSet(index)) {
          final List<Object> list = new JsonStringArrayList<>();
          final int start = offsets.getAccessor().get(index);
          final int until = offsets.getAccessor().get(index + 1);
          for (int i = start; i < until; i++) {
            Object value = vector.getAccessor().getObject(i);
            if (value == null && !vector.getAccessor().isNull(i)) {
              list.add(new JsonStringArrayList<>());
            } else {
              list.add(value);
            }
          }
          return list;
        } else {
          return null;
        }
      }

      public boolean isSet(int index) {
        return bits.getAccessor().get(index) == 1 || bits.getAccessor().get(index) == 3;
      }

      public void get(int index, RepeatedListHolder holder) {
        assert index <= getValueCapacity();
        holder.start = getOffsetVector().getAccessor().get(index);
        holder.end = getOffsetVector().getAccessor().get(index+1);
      }

      public void get(int index, ComplexHolder holder) {
        final FieldReader reader = getReader();
        reader.setPosition(index);
        holder.reader = reader;
      }

      public void get(int index, int arrayIndex, ComplexHolder holder) {
        final RepeatedListHolder listHolder = new RepeatedListHolder();
        get(index, listHolder);
        int offset = listHolder.start + arrayIndex;
        if (offset >= listHolder.end) {
          holder.reader = NullReader.INSTANCE;
        } else {
          FieldReader r = getDataVector().getReader();
          r.setPosition(offset);
          holder.reader = r;
        }
      }

      @Override
      public boolean isNull(int index) {
        return bits.getAccessor().get(index) == 2;
      }
    }

    public class RepeatedListMutator extends BaseRepeatedValueVector.BaseRepeatedMutator {

      public int add(int index) {
        bits.getMutator().set(index, 1);
        final int curEnd = getOffsetVector().getAccessor().get(index+1);
        getOffsetVector().getMutator().setSafe(index + 1, curEnd + 1);
        return curEnd;
      }

      @Override
      public void startNewValue(int index) {
        bits.getMutator().set(index, 1);
        emptyPopulator.populate(index+1);
        super.startNewValue(index);
      }

      @Override
      public void setValueCount(int valueCount) {
        bits.getMutator().setValueCount(valueCount);
        emptyPopulator.populate(valueCount);
        super.setValueCount(valueCount);
      }

      @Override
      public void writeNull(int index) {
        bits.getMutator().set(index, 2);
      }

      public void setNulls(AnyVector nullsVector) {
        nullsVector.getBits().transferTo(bits);
        setValueCount(nullsVector.getAccessor().getValueCount() + 1);
//        for (int i = 0; i < nullsVector.getAccessor().getValueCount(); i++) {
//          if (nullsVector.getAccessor().isSet(i)) {
//            startNewValue(i);
//          }
//        }
        nullsVector.clear();
      }
    }


    public class DelegateTransferPair implements TransferPair {
      private final DelegateRepeatedVector target;
      private final TransferPair[] children;

      public DelegateTransferPair(DelegateRepeatedVector target) {
        this.target = Preconditions.checkNotNull(target);
        if (target.getDataVector() == DEFAULT_DATA_VECTOR) {
          target.addOrGetVector(VectorDescriptor.create(getDataVector().getField()));
          target.getDataVector().allocateNew();
        }
        this.children = new TransferPair[] {
            getOffsetVector().makeTransferPair(target.getOffsetVector()),
            getDataVector().makeTransferPair(target.getDataVector())
        };
      }

      @Override
      public void transfer() {
        for (TransferPair child:children) {
          child.transfer();
        }
        bits.transferTo(target.bits);
        bits.clear();
      }

      @Override
      public ValueVector getTo() {
        return target;
      }

      @Override
      public void splitAndTransfer(int startIndex, int length) {
        target.allocateNew();
        for (int i = 0; i < length; i++) {
          copyValueSafe(startIndex + i, i);
        }
//        bits.splitAndTransferTo(startIndex, length, target.bits);
      }

      @Override
      public void copyValueSafe(int srcIndex, int destIndex) {
        final RepeatedListHolder holder = new RepeatedListHolder();
        getAccessor().get(srcIndex, holder);
        target.emptyPopulator.populate(destIndex+1);
        final TransferPair vectorTransfer = children[1];
        int newIndex = target.getOffsetVector().getAccessor().get(destIndex);
        //todo: make this a bulk copy.
        for (int i = holder.start; i < holder.end; i++, newIndex++) {
          vectorTransfer.copyValueSafe(i, newIndex);
        }
        target.getOffsetVector().getMutator().setSafe(destIndex + 1, newIndex);
//        bits.splitAndTransferTo(srcIndex, destIndex, target.bits);
        target.bits.copyFromSafe(srcIndex, destIndex, bits);
      }
    }

    public DelegateRepeatedVector(String path, BufferAllocator allocator) {
      this(MaterializedField.create(path, TYPE), allocator);
    }

    public DelegateRepeatedVector(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
      emptyPopulator = new EmptyValuePopulator(getOffsetVector());
    }

    @Override
    public void allocateNew() throws OutOfMemoryException {
      if (!allocateNewSafe()) {
        throw new OutOfMemoryException();
      }
    }

    @Override
    public boolean allocateNewSafe() {
    /* boolean to keep track if all the memory allocation were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
      boolean success = false;
      try {
        if (!offsets.allocateNewSafe() || !bits.allocateNewSafe()) {
          return false;
        }
        success = vector.allocateNewSafe();
      } finally {
        if (!success) {
          clear();
        }
      }
      offsets.zeroVector();
      bits.zeroVector();
      return success;
    }

    @Override
    public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
      return makeTransferPair(new DelegateRepeatedVector(ref, allocator));
    }

    @Override
    public TransferPair makeTransferPair(ValueVector target) {
      return new DelegateTransferPair(DelegateRepeatedVector.class.cast(target));
    }

    @Override
    public RepeatedListAccessor getAccessor() {
      return accessor;
    }

    @Override
    public RepeatedListMutator getMutator() {
      return mutator;
    }

    @Override
    public FieldReader getReader() {
      throw new UnsupportedOperationException();
    }

    public void copyFromSafe(int fromIndex, int thisIndex, DelegateRepeatedVector from) {
      if(ephPair == null || ephPair.target != from) {
        ephPair = DelegateTransferPair.class.cast(from.makeTransferPair(this));
      }
      ephPair.copyValueSafe(fromIndex, thisIndex);
      bits.copyFromSafe(fromIndex, thisIndex, from.bits);
    }

    @Override
    public int getBufferSize() {
      if (getAccessor().getValueCount() == 0) {
        return 0;
      }
      return bits.getBufferSize() + offsets.getBufferSize() + vector.getBufferSize();
    }

    @Override
    public int getBufferSizeFor(int valueCount) {
      if (valueCount == 0) {
        return 0;
      }

      return bits.getBufferSizeFor(valueCount)
              + offsets.getBufferSizeFor(valueCount + 1)
              + vector.getBufferSizeFor(valueCount);
    }

    @Override
    public DrillBuf[] getBuffers(boolean clear) {
      final DrillBuf[] buffers = ObjectArrays.concat(bits.getBuffers(false),
          ObjectArrays.concat(offsets.getBuffers(false), vector.getBuffers(false), DrillBuf.class), DrillBuf.class);
      if (clear) {
        for (DrillBuf buffer:buffers) {
          buffer.retain();
        }
        clear();
      }
      return buffers;
    }

    @Override
    public void load(UserBitShared.SerializedField metadata, DrillBuf buffer) {
      final UserBitShared.SerializedField bitsMetadata = metadata.getChild(0);
      bits.load(bitsMetadata, buffer);

      final int bitsLength = bitsMetadata.getBufferLength();
      final UserBitShared.SerializedField offsetMetadata = metadata.getChild(1);
      final int offsetLength = offsetMetadata.getBufferLength();
      offsets.load(offsetMetadata, buffer.slice(bitsLength, offsetLength));

      final UserBitShared.SerializedField vectorMetadata = metadata.getChild(2);
      if (getDataVector() == DEFAULT_DATA_VECTOR) {
        addOrGetVector(VectorDescriptor.create(vectorMetadata.getMajorType()));
      }

      final int vectorLength = vectorMetadata.getBufferLength();
      vector.load(vectorMetadata, buffer.slice(bitsLength + offsetLength, vectorLength));
    }

    @Override
    protected UserBitShared.SerializedField.Builder getMetadataBuilder() {
      return getField().getAsBuilder()
        .setValueCount(getAccessor().getValueCount())
        .setBufferLength(getBufferSize())
        .addChild(bits.getMetadata())
        .addChild(offsets.getMetadata())
        .addChild(vector.getMetadata());
    }

    @Override
    public void clear() {
      offsets.clear();
      bits.clear();
      vector.clear();
      super.clear();
    }

    @Override
    public void close() {
      bits.close();
      super.close();
    }
  }

  protected class RepeatedListTransferPair implements TransferPair {
    private final TransferPair delegate;

    public RepeatedListTransferPair(TransferPair delegate) {
      this.delegate = delegate;
    }

    @Override
    public void transfer() {
      delegate.transfer();
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      delegate.splitAndTransfer(startIndex, length);
    }

    @Override
    public ValueVector getTo() {
      final DelegateRepeatedVector delegateVector = DelegateRepeatedVector.class.cast(delegate.getTo());
      return new RepeatedListVector(getField(), allocator, callBack, delegateVector);
    }

    @Override
    public void copyValueSafe(int from, int to) {
      delegate.copyValueSafe(from, to);
    }
  }

  public RepeatedListVector(String path, BufferAllocator allocator, CallBack callBack) {
    this(MaterializedField.create(path, TYPE), allocator, callBack);
  }

  public RepeatedListVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this(field, allocator, callBack, new DelegateRepeatedVector(field, allocator));
  }

  protected RepeatedListVector(MaterializedField field, BufferAllocator allocator, CallBack callBack, DelegateRepeatedVector delegate) {
    super(field, allocator, callBack);
    this.delegate = Preconditions.checkNotNull(delegate);

    final List<MaterializedField> children = Lists.newArrayList(field.getChildren());
    final int childSize = children.size();
    assert childSize < 4;
    final boolean hasChild = childSize > 0;
    if (hasChild) {
      // the last field is data field
      final MaterializedField child = children.get(childSize-1);
      addOrGetVector(VectorDescriptor.create(child));
    }
  }


    @Override
  public RepeatedListReaderImpl getReader() {
    return reader;
  }

  @Override
  public DelegateRepeatedVector.RepeatedListAccessor getAccessor() {
    return delegate.getAccessor();
  }

  @Override
  public DelegateRepeatedVector.RepeatedListMutator getMutator() {
    return delegate.getMutator();
  }

  @Override
  public void initialize(int index) {
    delegate.bits.getMutator().set(index, 1);
  }

  @Override
  public UInt4Vector getOffsetVector() {
    return delegate.getOffsetVector();
  }

  @Override
  public ValueVector getDataVector() {
    return delegate.getDataVector();
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    delegate.allocateNew();
  }

  @Override
  public boolean allocateNewSafe() {
    return delegate.allocateNewSafe();
  }

  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(VectorDescriptor descriptor) {
    final AddOrGetResult<T> result = delegate.addOrGetVector(descriptor);
    if (result.isCreated() && callBack != null) {
      callBack.doWork();
    }
    this.field = delegate.getField();
    return result;
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public int getBufferSize() {
    return delegate.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    return delegate.getBufferSizeFor(valueCount);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new RepeatedListTransferPair(delegate.getTransferPair(allocator));
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new RepeatedListTransferPair(delegate.getTransferPair(ref, allocator));
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    final RepeatedListVector target = RepeatedListVector.class.cast(to);
    return new RepeatedListTransferPair(delegate.makeTransferPair(target.delegate));
  }

  @Override
  public int getValueCapacity() {
    return delegate.getValueCapacity();
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    return delegate.getBuffers(clear);
  }


  @Override
  public void load(SerializedField metadata, DrillBuf buf) {
    delegate.load(metadata, buf);
  }

  @Override
  public SerializedField getMetadata() {
    return delegate.getMetadata();
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return delegate.iterator();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    delegate.setInitialCapacity(numRecords);
  }

  /**
   * @deprecated
   *   prefer using {@link #addOrGetVector(org.apache.drill.exec.vector.VectorDescriptor)} instead.
   */
  @Override
  public <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz) {
    final AddOrGetResult<T> result = addOrGetVector(VectorDescriptor.create(type));
    return result.getVector();
  }

  @Override
  public <T extends ValueVector> T getChild(String name, Class<T> clazz) {
    if (name != null) {
      return null;
    }
    return typeify(delegate.getDataVector(), clazz);
  }

  @Override
  public void allocateNew(int valueCount, int innerValueCount) {
    clear();
    getOffsetVector().allocateNew(valueCount + 1);
    getMutator().reset();
  }

  @Override
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) {
    if (name != null) {
      return null;
    }
    return new VectorWithOrdinal(delegate.getDataVector(), 0);
  }

  public void copyFromSafe(int fromIndex, int thisIndex, RepeatedListVector from) {
    delegate.copyFromSafe(fromIndex, thisIndex, from.delegate);
  }
}
