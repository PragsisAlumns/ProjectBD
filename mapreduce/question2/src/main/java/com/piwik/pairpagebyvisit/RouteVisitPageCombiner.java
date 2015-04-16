package com.piwik.pairpagebyvisit;

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


public class RouteVisitPageCombiner extends Reducer<RouteIdVisitWritable, NullWritable, RouteIdVisitWritable, NullWritable> {
	
	@Override
	public void reduce(RouteIdVisitWritable key, Iterable<NullWritable> values, Context context) throws IOException,
			InterruptedException {

			context.write(key, NullWritable.get());	
	}
	
}
