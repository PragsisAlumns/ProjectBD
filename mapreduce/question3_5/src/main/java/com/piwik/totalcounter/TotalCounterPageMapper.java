package com.piwik.totalcounter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.piwik.common.IntPairWritable;

public class TotalCounterPageMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	Logger logger = Logger.getLogger(TotalCounterPageMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Splitting idvisit and paths 
		String[] fields = value.toString().split("\t");
				
		//Id visit 
		long idvisit = Long.parseLong(fields[0]);

		//Paths
		String[] singlePaths = fields[1].split("#");
				
		//Generating output
		for (String path : singlePaths ){ //for each path
			String[] page = path.split(","); //Split by pages
			
			HashMap<String,Long> tempValue = new HashMap<String,Long>();
					
			//Generating convert values
			int iter = 0;
			while (iter<page.length){
				if (tempValue.get(page[iter]) == null){
					context.write(new Text(page[iter]), new LongWritable(1));
					tempValue.put(page[iter], 1L);
				}	
				iter++;
			}	
		}
	}
}