package com.piwik.routesbyvisit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.piwik.common.IntPairWritable;
import com.piwik.common.RouteIdVisitWritable;


public class RouteByIdVisitMapper extends Mapper<LongWritable, Text, RouteIdVisitWritable, NullWritable> {
	Logger logger = Logger.getLogger(RouteByIdVisitMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Splitting lines 
		String[] lines = value.toString().split("#");
		
		//Finding idvisit and the first path
		int indexSeparator = lines[0].indexOf(",");
		long idvisit = Long.parseLong(lines[0].substring(0, indexSeparator));
		String firstPath = lines[0].substring(indexSeparator+1,lines[0].length());
		
		//
		String[] singlePaths = lines;
		singlePaths[0] = firstPath;
		
		String route;
		
		for (String path : singlePaths ){ //for each path
			String[] pages = path.split(","); //Split by pages
			for (int i=0; i<pages.length-1; i++){
				route = "";
				for (int f=i+1; f<pages.length; f++){
					route = route.concat(pages[i]);
					for (int t=i+1; t<=f; t++){
						route = route.concat(","+pages[t]);
					}
					// Emit pages couple 
					context.write(new RouteIdVisitWritable(route,idvisit), NullWritable.get());
					route = "";
				}
			}
		}
	}

}
