package com.piwik.averagepage;

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


public class AveragePageReducer extends Reducer<Text, LongWritable, Text, Text> {
	long counterConvertUsers;
	long totalVisitPages;
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
			InterruptedException {
		
		long currentValue =  values.iterator().next().get();
		
		//Total of pages visited by user increasing the total
		totalVisitPages = totalVisitPages + currentValue;
		
		//Increasing total of users
		counterConvertUsers++;
		
	}
	
	@Override
	public void run(Context context) throws IOException, InterruptedException {
		  setup(context);
		  counterConvertUsers = 0;
		  totalVisitPages = 0;
		  
		  while (context.nextKey()) {
		    reduce(context.getCurrentKey(), context.getValues(), context);
		  }
		  
		  String average = Long.toString((totalVisitPages/counterConvertUsers));
		  
		  context.write(new Text("Page visit average by converted user: "), new Text(average));
		  cleanup(context);
		}

	
	
}
