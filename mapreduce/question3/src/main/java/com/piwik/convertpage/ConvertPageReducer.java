package com.piwik.convertpage;

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


public class ConvertPageReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
			InterruptedException {
		
		long totalValue = 0;
		//Sum of values
		for (LongWritable value : values){
			totalValue = totalValue + value.get();
		}
		
	    //Writing value
		context.write(key, new LongWritable(totalValue));
	
	}
	
	
}
