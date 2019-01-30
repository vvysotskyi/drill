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
package org.apache.drill.exec.vector.complex;

import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.BasicTypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.RepeatedMapHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.AddOrGetResult;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.AnyVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorDescriptor;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.RepeatedMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.google.common.collect.Maps;

public class RepeatedMapVector extends AbstractMapVector
    implements RepeatedValueVector, RepeatedFixedWidthVectorLike {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RepeatedMapVector.class);

  public final static MajorType TYPE = MajorType.newBuilder().setMinorType(MinorType.MAP).setMode(DataMode.REPEATED).build();

  private final UInt4Vector offsets;   // offsets to start of each record (considering record indices are 0-indexed)
  private final RepeatedMapReaderImpl reader = new RepeatedMapReaderImpl(RepeatedMapVector.this);
  private final RepeatedMapAccessor accessor = new RepeatedMapAccessor();
  private final Mutator mutator = new Mutator();
  private final EmptyValuePopulator emptyPopulator;
  private final MaterializedField bitsField = MaterializedField.create("$bits$", Types.required(TypeProtos.MinorType.UINT1));
  private final UInt1Vector bits = new UInt1Vector(bitsField, allocator);

  public RepeatedMapVector(MaterializedField field, BufferAllocator allocator, CallBack callBack){
    super(field, allocator, callBack);
    this.offsets = new UInt4Vector(BaseRepeatedValueVector.OFFSETS_FIELD, allocator);
    this.emptyPopulator = new EmptyValuePopulator(offsets);
  }

  @Override
  public UInt4Vector getOffsetVector() {
    return offsets;
  }

  @Override
  public ValueVector getDataVector() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(VectorDescriptor descriptor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    offsets.setInitialCapacity(numRecords + 1);
    for(final ValueVector v : (Iterable<ValueVector>) this) {
      v.setInitialCapacity(numRecords * RepeatedValueVector.DEFAULT_REPEAT_PER_RECORD);
    }
  }

  @Override
  public RepeatedMapReaderImpl getReader() {
    return reader;
  }

  @Override
  public void allocateNew(int groupCount, int innerValueCount) {
    clear();
    try {
      offsets.allocateNew(groupCount + 1);
      bits.allocateNew(groupCount + 1);
      for (ValueVector v : getChildren()) {
        AllocationHelper.allocatePrecomputedChildCount(v, groupCount, 50, innerValueCount);
      }
    } catch (OutOfMemoryException e){
      clear();
      throw e;
    }
    bits.zeroVector();
    offsets.zeroVector();
    mutator.reset();
  }

  public Iterator<String> fieldNameIterator() {
    return getChildFieldNames().iterator();
  }

  @Override
  public List<ValueVector> getPrimitiveVectors() {
    final List<ValueVector> primitiveVectors = super.getPrimitiveVectors();
    primitiveVectors.add(offsets);
    return primitiveVectors;
  }

  @Override
  public int getBufferSize() {
    if (getAccessor().getValueCount() == 0) {
      return 0;
    }
    long bufferSize = offsets.getBufferSize();
    bufferSize += bits.getBufferSize();
    for (final ValueVector v : this) {
      bufferSize += v.getBufferSize();
    }
    return (int) bufferSize;
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = 0;
    for (final ValueVector v : this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize;
  }

  @Override
  public void close() {
    offsets.close();
    bits.close();
    super.close();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new RepeatedMapTransferPair(this, getField().getPath(), allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new RepeatedMapTransferPair(this, (RepeatedMapVector)to);
  }

  MapSingleCopier makeSingularCopier(MapVector to) {
    return new MapSingleCopier(this, to);
  }

  protected static class MapSingleCopier {
    private final TransferPair[] pairs;
    public final RepeatedMapVector from;

    public MapSingleCopier(RepeatedMapVector from, MapVector to) {
      this.from = from;
      this.pairs = new TransferPair[from.size()];

      int i = 0;
      ValueVector vector;
      for (final String child:from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }

    public void copySafe(int fromSubIndex, int toIndex) {
      for (TransferPair p : pairs) {
        p.copyValueSafe(fromSubIndex, toIndex);
      }
    }
  }

  public TransferPair getTransferPairToSingleMap(String reference, BufferAllocator allocator) {
    return new SingleMapTransferPair(this, reference, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new RepeatedMapTransferPair(this, ref, allocator);
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
      success =  super.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    offsets.zeroVector();
    bits.zeroVector();
    return success;
  }

  protected static class SingleMapTransferPair implements TransferPair {
    private final TransferPair[] pairs;
    private final RepeatedMapVector from;
    private final MapVector to;
    private static final MajorType MAP_TYPE = Types.required(MinorType.MAP);

    public SingleMapTransferPair(RepeatedMapVector from, String path, BufferAllocator allocator) {
      this(from, new MapVector(MaterializedField.create(path, MAP_TYPE), allocator, new SchemaChangeCallBack()), false);
    }

    public SingleMapTransferPair(RepeatedMapVector from, MapVector to) {
      this(from, to, true);
    }

    public SingleMapTransferPair(RepeatedMapVector from, MapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      int i = 0;
      ValueVector vector;
      for (final String child : from.getChildFieldNames()) {
        int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }
        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (allocate && to.size() != preSize) {
          newVector.allocateNew();
        }
        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }


    @Override
    public void transfer() {
      for (TransferPair p : pairs) {
        p.transfer();
      }
//      from.bits.getMutator().set();
      to.getMutator().setValueCount(from.getAccessor().getValueCount());
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      for (TransferPair p : pairs) {
        p.copyValueSafe(from, to);
      }
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      for (TransferPair p : pairs) {
        p.splitAndTransfer(startIndex, length);
      }
      to.getMutator().setValueCount(length);
    }
  }

  private static class RepeatedMapTransferPair implements TransferPair{

    private final TransferPair[] pairs;
    private final RepeatedMapVector to;
    private final RepeatedMapVector from;

    public RepeatedMapTransferPair(RepeatedMapVector from, String path, BufferAllocator allocator) {
      this(from, new RepeatedMapVector(MaterializedField.create(path, TYPE), allocator, new SchemaChangeCallBack()), false);
    }

    public RepeatedMapTransferPair(RepeatedMapVector from, RepeatedMapVector to) {
      this(from, to, true);
    }

    public RepeatedMapTransferPair(RepeatedMapVector from, RepeatedMapVector to, boolean allocate) {
      this.from = from;
      this.to = to;
      this.pairs = new TransferPair[from.size()];
      this.to.ephPair = null;

      int i = 0;
      ValueVector vector;
      for (final String child : from.getChildFieldNames()) {
        final int preSize = to.size();
        vector = from.getChild(child);
        if (vector == null) {
          continue;
        }

        final ValueVector newVector = to.addOrGet(child, vector.getField().getType(), vector.getClass());
        if (to.size() != preSize) {
          newVector.allocateNew();
        }

        pairs[i++] = vector.makeTransferPair(newVector);
      }
    }

    @Override
    public void transfer() {
      from.offsets.transferTo(to.offsets);
      for (TransferPair p : pairs) {
        p.transfer();
      }
      from.bits.transferTo(to.bits);
      from.clear();
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int srcIndex, int destIndex) {
      RepeatedMapHolder holder = new RepeatedMapHolder();
      from.getAccessor().get(srcIndex, holder);
      to.emptyPopulator.populate(destIndex + 1);
      int newIndex = to.offsets.getAccessor().get(destIndex);
      //todo: make these bulk copies
      for (int i = holder.start; i < holder.end; i++, newIndex++) {
        for (TransferPair p : pairs) {
          p.copyValueSafe(i, newIndex);
        }
      }
      to.offsets.getMutator().setSafe(destIndex + 1, newIndex);
    }

    @Override
    public void splitAndTransfer(final int groupStart, final int groups) {
      final UInt4Vector.Accessor a = from.offsets.getAccessor();
      final UInt4Vector.Mutator m = to.offsets.getMutator();

      final int startPos = a.get(groupStart);
      final int endPos = a.get(groupStart + groups);
      final int valuesToCopy = endPos - startPos;

      to.offsets.clear();
      to.offsets.allocateNew(groups + 1);
      from.bits.splitAndTransferTo(groupStart, groups, to.bits);

      int normalizedPos;
      for (int i = 0; i < groups + 1; i++) {
        normalizedPos = a.get(groupStart + i) - startPos;
        m.set(i, normalizedPos);
      }

      m.setValueCount(groups + 1);
      to.emptyPopulator.populate(groups);

      for (final TransferPair p : pairs) {
        p.splitAndTransfer(startPos, valuesToCopy);
      }
    }
  }


  transient private RepeatedMapTransferPair ephPair;

  public void copyFromSafe(int fromIndex, int thisIndex, RepeatedMapVector from) {
    if (ephPair == null || ephPair.from != from) {
      ephPair = (RepeatedMapTransferPair) from.makeTransferPair(this);
    }
    ephPair.copyValueSafe(fromIndex, thisIndex);
  }

  @Override
  public int getValueCapacity() {
    return Math.max(offsets.getValueCapacity() - 1, 0);
  }

  @Override
  public RepeatedMapAccessor getAccessor() {
    return accessor;
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    //final int expectedBufferSize = getBufferSize();
    //final int actualBufferSize = super.getBufferSize();
    //Preconditions.checkArgument(expectedBufferSize == actualBufferSize + offsets.getBufferSize());
    return ArrayUtils.addAll(bits.getBuffers(clear), ArrayUtils.addAll(offsets.getBuffers(clear), super.getBuffers(clear)));
  }


  @Override
  public void load(SerializedField metadata, DrillBuf buffer) {
    final List<SerializedField> children = metadata.getChildList();

    final UserBitShared.SerializedField bitsMetadata = children.get(0);
    bits.load(bitsMetadata, buffer);

    final int bitsLength = bitsMetadata.getBufferLength();
    final SerializedField offsetField = children.get(1);
    final int offsetLength = offsetField.getBufferLength();
    offsets.load(offsetField, buffer.slice(bitsLength, offsetLength));
    int bufOffset = bitsLength + offsetLength;

    for (int i = 2; i < children.size(); i++) {
      final SerializedField child = children.get(i);
      final MaterializedField fieldDef = MaterializedField.create(child);
      ValueVector vector = getChild(fieldDef.getLastName());
      if (vector == null) {
        // if we arrive here, we didn't have a matching vector.
        vector = BasicTypeHelper.getNewVector(fieldDef, allocator);
        putChild(fieldDef.getLastName(), vector);
      }
      final int vectorLength = child.getBufferLength();
      vector.load(child, buffer.slice(bufOffset, vectorLength));
      bufOffset += vectorLength;
    }

    assert bufOffset == buffer.capacity();
  }


  @Override
  public SerializedField getMetadata() {
    SerializedField.Builder builder = getField() //
        .getAsBuilder() //
        .setBufferLength(getBufferSize()) //
        // while we don't need to actually read this on load, we need it to make sure we don't skip deserialization of this vector
        .setValueCount(accessor.getValueCount());
    builder.addChild(bits.getMetadata());
    builder.addChild(offsets.getMetadata());
    for (final ValueVector child : getChildren()) {
      builder.addChild(child.getMetadata());
    }
    return builder.build();
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public void initialize(int index) {
    bits.getMutator().set(index, 1);
    mutator.setValueCount(index + 1);
  }

  public class RepeatedMapAccessor implements RepeatedAccessor {
    @Override
    public Object getObject(int index) {
      if (bits.getAccessor().get(index) == 1 || bits.getAccessor().get(index) == 3) {
        final List<Object> list = new JsonStringArrayList<>();
        final int end = offsets.getAccessor().get(index+1);
        String fieldName;
        for (int i =  offsets.getAccessor().get(index); i < end; i++) {
          final Map<String, Object> vv = Maps.newLinkedHashMap();
          for (final MaterializedField field : getField().getChildren()) {
            if (!field.equals(BaseRepeatedValueVector.OFFSETS_FIELD) && !field.equals(bitsField)) {
              fieldName = field.getLastName();
              Accessor childAccessor = getChild(fieldName).getAccessor();
              final Object value = childAccessor.getObject(i);
              if (value != null || childAccessor.isNull(i)) {
                vv.put(fieldName, value);
              }
            }
          }
          list.add(vv);
        }
        return list;
      } else {
        return null;
      }
    }

    @Override
    public int getValueCount() {
      return Math.max(offsets.getAccessor().getValueCount() - 1, 0);
    }

    @Override
    public int getInnerValueCount() {
      final int valueCount = getValueCount();
      if (valueCount == 0) {
        return 0;
      }
      return offsets.getAccessor().get(valueCount);
    }

    @Override
    public int getInnerValueCountAt(int index) {
      return offsets.getAccessor().get(index+1) - offsets.getAccessor().get(index);
    }

    @Override
    public boolean isEmpty(int index) {
      return false;
    }

    @Override
    public boolean isNull(int index) {
      return bits.getAccessor().get(index) == 2;
    }

    public void get(int index, RepeatedMapHolder holder) {
      assert index < getValueCapacity() :
        String.format("Attempted to access index %d when value capacity is %d",
            index, getValueCapacity());
      final UInt4Vector.Accessor offsetsAccessor = offsets.getAccessor();
      holder.start = offsetsAccessor.get(index);
      holder.end = offsetsAccessor.get(index + 1);
    }

    public void get(int index, ComplexHolder holder) {
      final FieldReader reader = getReader();
      reader.setPosition(index);
      holder.reader = reader;
    }

    public void get(int index, int arrayIndex, ComplexHolder holder) {
      final RepeatedMapHolder h = new RepeatedMapHolder();
      get(index, h);
      final int offset = h.start + arrayIndex;

      if (offset >= h.end) {
        holder.reader = NullReader.INSTANCE;
      } else {
        reader.setSinglePosition(index, arrayIndex);
        holder.reader = reader;
      }
    }
  }

  public class Mutator implements RepeatedMutator {
    @Override
    public void startNewValue(int index) {
      emptyPopulator.populate(index + 1);
      offsets.getMutator().setSafe(index + 1, offsets.getAccessor().get(index));
      bits.getMutator().setSafe(index, 1);
    }

    @Override
    public void setValueCount(int topLevelValueCount) {
      emptyPopulator.populate(topLevelValueCount);
      offsets.getMutator().setValueCount(topLevelValueCount == 0 ? 0 : topLevelValueCount + 1);
      bits.getMutator().setValueCount(topLevelValueCount);
      int childValueCount = offsets.getAccessor().get(topLevelValueCount);
      for (final ValueVector v : getChildren()) {
        v.getMutator().setValueCount(childValueCount);
      }
    }

    @Override
    public void reset() {}

    @Override
    public void generateTestData(int values) {}

    public int add(int index) {
      final int prevEnd = offsets.getAccessor().get(index + 1);
      offsets.getMutator().setSafe(index + 1, prevEnd + 1);
      bits.getMutator().setSafe(index, 1);
      return prevEnd;
    }

    @Override
    public void writeNull(int index) {
      bits.getMutator().set(index, 2);
    }

    public void setNotNull(int index) {
      bits.getMutator().setSafe(index, 1);
    }

    public void setNulls(AnyVector nullsVector) {
      nullsVector.getBits().transferTo(bits);
      setValueCount(nullsVector.getAccessor().getValueCount() + 1);
//      for (int i = 0; i < nullsVector.getAccessor().getValueCount(); i++) {
//        if (nullsVector.getAccessor().isSet(i)) {
//          startNewValue(i);
//        }
//      }
      nullsVector.clear();
    }
  }

  @Override
  public void clear() {
    getMutator().reset();

    offsets.clear();
    bits.clear();
    for(final ValueVector vector : getChildren()) {
      vector.clear();
    }
  }
}
