package com.piwik.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PairPageVisitWritable implements WritableComparable<PairPageVisitWritable> {

  String pair;
  long idVisit;

  /**
   * Empty constructor - required for serialization.
   */ 
  public PairPageVisitWritable() {

  }

  /**
   * Constructor with two integers provided as input.
   */ 
  public PairPageVisitWritable(String route, long idVisit) {
    this.pair = route;
    this.idVisit = idVisit;
  }

  /**
   * Serializes the fields of this object to out.
   */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(pair);
    out.writeLong(idVisit);
  }

  /**
   * Deserializes the fields of this object from in.
   */
  public void readFields(DataInput in) throws IOException {
    pair = in.readUTF();
    idVisit = in.readLong();
  }

  /**
   * Compares this object to another IntPairWritable object by
   * comparing the left integer first. If the left integer are equal,
   * then the right integer are compared.
   */
  public int compareTo(PairPageVisitWritable other) {
	  int result = pair.compareTo(other.pair);
	  if (result == 0) {
		  result = (int) (idVisit - other.idVisit);
	    }
	 
    return result;
  }

  /* getters and setters for the two objects in the pair */
  public String getRoute() {
	  return pair;
  }
  
  public long getIdVisit() {
	  return idVisit;
  }
  
  public void  setRoute(String route) {
	  this.pair = route;
  }
  
  public void setIdVisit(int idVisit) {
	  this.idVisit = idVisit;
  }

  /**
   * A custom method that returns the two integers in the 
   * IntPairWritable object inside parentheses and separated by
   * a comma. For example: "(left,right)".
   */
  public String toString() {
    return "(" + pair + "," + idVisit + ")";
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
  	PairPageVisitWritable other = (PairPageVisitWritable) obj;
  	if (idVisit != other.idVisit)
  		return false;
  	if (pair != other.pair)
  		return false;
  	return true;
  }
  
  
  @Override
  public int hashCode() {
  	final int prime = 31;
  	int result = 1;
  	result = prime * result + (int)idVisit;
  	result = prime * result + ((pair == null) ? 0 : pair.hashCode());
  	return result;
  }

}
