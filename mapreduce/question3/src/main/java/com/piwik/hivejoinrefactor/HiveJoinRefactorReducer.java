package com.piwik.hivejoinrefactor;

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


public class HiveJoinRefactorReducer extends Reducer<Text, Text, Text, Text> {

	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		
		long totalValue = 0;
		String[] keyFields = key.toString().split("!");
		
		String convertPages = "";
		//Sum of values
		for (Text value : values){
			convertPages = convertPages.concat(value.toString()+",") ;
		}
		convertPages = convertPages.substring(0, convertPages.length()-1);
		
	    //Writing value
		context.write(new Text(keyFields[0]+""+convertPages+""+keyFields[1]), new Text(" "));
	}
	
	
}
