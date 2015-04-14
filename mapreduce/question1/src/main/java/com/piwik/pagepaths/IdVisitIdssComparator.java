package com.piwik.pagepaths;


import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class IdVisitIdssComparator extends WritableComparator {

	public IdVisitIdssComparator() {
		super(WritableComparable.class);
	}

	/** Overrides the default comparison of WritableComparables to compare 
	 * StringPairWritables that contain a Name in the first/left field
	 * and a Year in the second/right field.  
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable v1, WritableComparable v2){

		IntPairWritable pair1 = (IntPairWritable)v1;
		IntPairWritable pair2 = (IntPairWritable)v2;
		
		//  compare the left integer (idvisit) of both pairs.  		
		int integerCompare = pair2.getLeft() - pair1.getLeft();
		
		if (integerCompare == 0) {
			// If idvisit are the same, compare the right id(idss), ranking ids
			// in descending order
			return pair1.getRight()- pair2.getRight();
		}
		else {
			// If names are different, we're done, return the comparison
			return integerCompare;
		}
	}		
	
	
	/** 
	 * Overrides the default compare method, which is optimized for objects which
	 * can be compared byte by byte.  For Name/Year this isn't the case, so we need
	 * to read the incoming bytes to deserialize the StringPairWritable objects being
	 * compared.
	 */
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		DataInput stream1 = new DataInputStream(new ByteArrayInputStream(b1,
				s1, l1));
		DataInput stream2 = new DataInputStream(new ByteArrayInputStream(b2,
				s2, l2));

		IntPairWritable v1 = new IntPairWritable();
		IntPairWritable v2 = new IntPairWritable();

		try {
			v1.readFields(stream1);
			v2.readFields(stream2);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return compare(v1, v2);
	}

}
