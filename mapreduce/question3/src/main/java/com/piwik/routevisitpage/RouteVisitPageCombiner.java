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

import com.piwik.common.PageRouteWritable;


public class RouteVisitPageCombiner extends Reducer<PageRouteWritable, LongWritable, PageRouteWritable, LongWritable> {
	
	@Override
	public void reduce(PageRouteWritable key, Iterable<LongWritable> values, Context context) throws IOException,
			InterruptedException {
			
			long counter = 0;
		
			for (LongWritable count : values){
				counter = counter + count.get();
			}
		
			context.write(key, new LongWritable(counter));	
	}
	
}
