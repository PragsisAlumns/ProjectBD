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
import com.piwik.common.RouteIdVisitWritable;


public class RouteVisitPageReducer extends Reducer<Text, Text, Text, LongWritable> {

	@Override
	public void reduce(Text key, Text values, Context context) throws IOException,
			InterruptedException {
		
		String newRoute = key.getRoute();
		
	    //Counting the number of pages
		if (lastRoute.equals(newRoute)){
			counter++;
		}else {
			if (!firstTime){
				context.write(new Text(lastRoute), new LongWritable(counter));	
			}else {
				firstTime=false;
			}
			lastRoute = key.getRoute();
			counter = 1;
		}
	}
	
}
