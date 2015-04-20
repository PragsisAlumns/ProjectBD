package com.piwik.routevisitpage;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.piwik.common.PageRouteWritable;


public class RouteVisitPageReducer extends Reducer<PageRouteWritable, LongWritable, Text, Text> {
	long counterRoute;
	long counterVisit;
	String lastPage;
	boolean firstTime;
	
	@Override
	public void reduce(PageRouteWritable key, Iterable<LongWritable> values, Context context) throws IOException,
			InterruptedException {
		
		String newPage = key.getPage();
		long numberVisits = 0;
		
		//Counting visits by page and route for current key/value
		for (LongWritable nvisits : values){
			numberVisits = numberVisits + nvisits.get();
		}
		
	    //Counting the number of visits
		if (!firstTime){
			if (lastPage.equals(newPage)){
				counterVisit = counterVisit + numberVisits;
				counterRoute++;
			}else{
				context.write(new Text(lastPage), new Text(Long.toString(counterRoute)+","+counterVisit));
				counterRoute = 1;
				counterVisit = numberVisits;
			}
		}else {
			firstTime=false;
			counterRoute = 1;
			counterVisit = numberVisits;
		}
		lastPage = newPage;
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		  setup(context);
		  counterRoute = 0;
		  counterVisit = 0;
		  lastPage = "";
		  firstTime = true;
		  
		  while (context.nextKey()) {
		    reduce(context.getCurrentKey(), context.getValues(), context);
		  }
		  context.write(new Text(lastPage), new Text(Long.toString(counterRoute)+","+counterVisit));
		  cleanup(context);
		}

	
	
}
