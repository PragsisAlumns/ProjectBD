package com.piwik.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class RouteIdVisitWritable implements WritableComparable<RouteIdVisitWritable> {

  String route;
  long idVisit;

  /**
   * Empty constructor - required for serialization.
   */ 
  public RouteIdVisitWritable() {

  }

  /**
   * Constructor with two integers provided as input.
   */ 
  public RouteIdVisitWritable(String route, long idVisit) {
    this.route = route;
    this.idVisit = idVisit;
  }

  /**
   * Serializes the fields of this object to out.
   */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(route);
    out.writeLong(idVisit);
  }

  /**
   * Deserializes the fields of this object from in.
   */
  public void readFields(DataInput in) throws IOException {
    route = in.readUTF();
    idVisit = in.readLong();
  }

  /**
   * Compares this object to another IntPairWritable object by
   * comparing the left integer first. If the left integer are equal,
   * then the right integer are compared.
   */
  public int compareTo(RouteIdVisitWritable other) {
	  int result = route.compareTo(other.route);
	  if (result == 0) {
		  result = (int) (idVisit - other.idVisit);
	    }
	 
    return result;
  }

  /* getters and setters for the two objects in the pair */
  public String getRoute() {
	  return route;
  }
  
  public long getIdVisit() {
	  return idVisit;
  }
  
  public void  setRoute(String route) {
	  this.route = route;
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
    return "(" + route + "," + idVisit + ")";
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
  	RouteIdVisitWritable other = (RouteIdVisitWritable) obj;
  	if (idVisit != other.idVisit)
  		return false;
  	if (route != other.route)
  		return false;
  	return true;
  }
  
  
  @Override
  public int hashCode() {
  	final int prime = 31;
  	int result = 1;
  	result = prime * result + (int)idVisit;
  	result = prime * result + ((route == null) ? 0 : route.hashCode());
  	return result;
  }

}
