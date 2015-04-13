package com.piwik.pagepaths;

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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PagesPathsReducer extends Reducer<Text, Text, Text, LongWritable> {

	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		
		//Counting the number of pages
		Long visits = 0L;
		String totalUserIDs = "";
		Hashtable<String,String> totalUsers = new Hashtable<String,String>();
		
		for (Text id : values){
			if (totalUsers.get(id.toString()) == null){
				visits++;
				totalUsers.put(id.toString(),"1");
			}
		}
		
		context.write(new Text(key), new LongWritable(visits));
	}

	
}
