package com.piwik.convertpage;

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

public class ConvertPageMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	Logger logger = Logger.getLogger(ConvertPageMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Splitting idvisit and paths 
		String[] fields = value.toString().split("");
		
		//Id visit 
		long idvisit = Long.parseLong(fields[0]);

		//Convert page
		String convertPage = fields[1];
		
		//Paths
		String[] singlePaths = fields[2].split("#");
		String pairPage;
		
		//Generating output
		for (String path : singlePaths ){ //for each path
			String[] page = path.split(","); //Split by pages
			int position = -1;
			HashMap<String,Long> tempValue = new HashMap<String,Long>();
			
			//Looking for the last position for convert page
			for (int i = page.length-1; i >= 0; i-- ){
				if (page[i].equals(convertPage)){
					position = i;
					break;
				}
			}
			
			//Generating convert values
			int iter = 0;
			while (iter<=position){
				if (tempValue.get(page[iter]) == null){
					context.write(new Text(page[iter]), new LongWritable(1));
					tempValue.put(page[iter], 1L);
				}	
				iter++;
			}	
		}
	}
}