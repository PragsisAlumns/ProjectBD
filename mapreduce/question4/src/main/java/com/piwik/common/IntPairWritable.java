package com.piwik.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPairWritable implements WritableComparable<IntPairWritable> {

  int idVisit;
  int idSs;

  /**
   * Empty constructor - required for serialization.
   */ 
  public IntPairWritable() {

  }

  /**
   * Constructor with two integers provided as input.
   */ 
  public IntPairWritable(int left, int right) {
    this.idVisit = left;
    this.idSs = right;
  }

  /**
   * Serializes the fields of this object to out.
   */
  public void write(DataOutput out) throws IOException {
    out.writeInt(idVisit);
    out.writeInt(idSs);
  }

  /**
   * Deserializes the fields of this object from in.
   */
  public void readFields(DataInput in) throws IOException {
    idVisit = in.readInt();
    idSs = in.readInt();
  }

  /**
   * Compares this object to another IntPairWritable object by
   * comparing the left integer first. If the left integer are equal,
   * then the right integer are compared.
   */
  public int compareTo(IntPairWritable other) {
    int result = idVisit - other.idVisit;
    
    if (result == 0){
    	result = idSs - other.idSs;
    }
    return result;
  }

  /* getters and setters for the two objects in the pair */
  public int getLeft() {
	  return idVisit;
  }
  
  public int getRight() {
	  return idSs;
  }
  
  public void  setLeft(int left) {
	  this.idVisit = left;
  }
  
  public void setRight(int right) {
	  this.idSs = right;
  }

  /**
   * A custom method that returns the two integers in the 
   * IntPairWritable object inside parentheses and separated by
   * a comma. For example: "(left,right)".
   */
  public String toString() {
    return "(" + idVisit + "," + idSs + ")";
  }

  /**
   * The equals method compares two IntPairWritable objects for 
   * equality. The equals and hashCode methods have been automatically
   * generated by Eclipse by right-clicking on an empty line, selecting
   * Source, and then selecting the Generate hashCode() and equals()
   * option. 
   */
  @Override
  public boolean equals(Object obj) {
  	if (this == obj)
  		return true;
  	if (obj == null)
  		return false;
  	if (getClass() != obj.getClass())
  		return false;
  	IntPairWritable other = (IntPairWritable) obj;
  	if (idSs != other.idSs)
  		return false;
  	if (idVisit != other.idVisit)
  		return false;
  	return true;
  }
  
  
  @Override
  public int hashCode() {
  	final int prime = 31;
  	int result = 1;
  	result = prime * result + idSs;
  	result = prime * result + idVisit;
  	return result;
  }

}
