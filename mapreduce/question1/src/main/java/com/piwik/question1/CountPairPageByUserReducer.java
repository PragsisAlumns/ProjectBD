package com.piwik.question1;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountPairPageByUserReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void setup(Context context) throws IOException {
	
	}

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		/*EVERYTHING TO IMPLEMENT
			context.write(new Text(time), new Text(output));
		*/	
		
	}

	
}
