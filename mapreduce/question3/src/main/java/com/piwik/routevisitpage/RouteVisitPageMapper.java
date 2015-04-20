package com.piwik.routevisitpage;

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


public class RouteVisitPageMapper extends Mapper<LongWritable, Text, PageRouteWritable, LongWritable> {
	Logger logger = Logger.getLogger(RouteVisitPageMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Splitting idvisit and paths 
		String[] fields = value.toString().split("\t");
		
		//Id visit 
		long idvisit = Long.parseLong(fields[0]);

		//Paths
		String[] singlePaths = fields[1].split("#");
		String pairPage;
		
		//Route
		String route;
		
		//Generating output
		for (String path : singlePaths ){ //for each path
			String[] page = path.split(","); //Split by pages
			route = page[0];
			for (int i=1; i<page.length ; i++){
				route = route.concat(","+page[i]);
				context.write(new PageRouteWritable (page[i],route), new LongWritable(1));
			}
		}
	
	}

}
