

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.physical.impl.aggregate.InternalBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.BigIntVector;

public class StreamingAggregatorGen4 {

  BigIntHolder work0;
  BigIntHolder const1;
  BigIntHolder constant2;
  BigIntVector vv4;

  public void addRecord(int index)
      throws SchemaChangeException
  {
    {
      constant2 = new BigIntHolder();
      constant2 .value = const1 .value;
      BigIntHolder in = constant2;
      BigIntHolder value = work0;
        CountFunctions$BigIntCountFunction_add:
      {
        value.value++;
      }
      work0 = value;
    }
  }

  public int getVectorIndex(int recordIndex)
      throws SchemaChangeException
  {
    {
      return (recordIndex);
    }
  }

  public boolean isSame(int index1, int index2)
      throws SchemaChangeException
  {
    {
      return true;
    }
  }

  public boolean isSamePrev(int b1Index, InternalBatch b1, int b2Index)
      throws SchemaChangeException
  {
    {
      return true;
    }
  }

  public void outputRecordKeys(int inIndex, int outIndex)
      throws SchemaChangeException
  {
  }

  public void outputRecordKeysPrev(InternalBatch previous, int previousIndex, int outIndex)
      throws SchemaChangeException
  {
  }

  public void outputRecordValues(int outIndex)
      throws SchemaChangeException
  {
    {
      BigIntHolder out3;
      {
        final BigIntHolder out = new BigIntHolder();
        BigIntHolder value = work0;
          CountFunctions$BigIntCountFunction_output:
        {
          out.value = value.value;
        }
        work0 = value;
        out3 = out;
      }
      vv4 .getMutator().setSafe((outIndex), out3 .value);
    }
  }

  public boolean resetValues()
      throws SchemaChangeException
  {
    {
      /** start RESET for function count **/
      {
        BigIntHolder value = work0;
          CountFunctions$BigIntCountFunction_reset:
        {
          value.value = 0;
        }
        work0 = value;
      }
      /** end RESET for function count **/
      return true;
    }
  }

  public void setupInterior(RecordBatch incoming, RecordBatch outgoing)
      throws SchemaChangeException
  {
    {
      /** start SETUP for function count **/
      {
        BigIntHolder value = work0;
          CountFunctions$BigIntCountFunction_setup:
        {
          value = new BigIntHolder();
          value.value = 0;
        }
        work0 = value;
      }
      /** end SETUP for function count **/
      int[] fieldIds5 = new int[ 1 ] ;
      fieldIds5 [ 0 ] = 0;
      Object tmp6 = (outgoing).getValueAccessorById(BigIntVector.class, fieldIds5).getValueVector();
      if (tmp6 == null) {
        throw new SchemaChangeException("Failure while loading vector vv4 with id: TypedFieldId [fieldIds=[0], remainder=null].");
      }
      vv4 = ((BigIntVector) tmp6);
    }
  }

  public void __DRILL_INIT__()
      throws SchemaChangeException
  {
    {
      work0 = new BigIntHolder();
    }
  }

}
