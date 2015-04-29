package com.piwik.averagepage;

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
import com.piwik.common.PageRouteWritable;


public class AveragePageMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	Logger logger = Logger.getLogger(AveragePageMapper.class);
	
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
				
				int totalPages = 0;
				long allPages = 0;
				
				//Counting the total of pages visited
				for (String path : singlePaths ){ //for each path
					String[] pages = path.split(",");
					allPages = allPages+pages.length;
				}	
				
				context.write(new Text(Long.toString(idvisit)), new LongWritable(allPages));
			}

}
