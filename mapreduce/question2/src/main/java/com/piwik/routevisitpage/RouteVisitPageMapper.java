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
import com.piwik.common.RouteIdVisitWritable;


public class RouteVisitPageMapper extends Mapper<LongWritable, Text, Text, Text> {
	Logger logger = Logger.getLogger(RouteVisitPageMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		
		Map<String,Long> pages = new HashMap<String,Long>();
		
		//Splitting idvisit and paths 
		String[] fields = value.toString().split("\t");
		
		//Id visit 
		long idvisit = Long.parseLong(fields[0]);

		//Paths
		String[] singlePaths = fields[1].split("#");
		String pairPage;
		
		for (String path : singlePaths ){ //for each path
			String[] page = path.split(","); //Split by pages
			for (int i=1; i<page.length-1; i++){
				Long currentPageValue = pages.get(page[i]);
				if (currentPageValue != null){
					pages.put(page[i],currentPageValue+1);
				} else {
					pages.put(page[i], 1L);
				}
			}
		}
		Iterator<Map.Entry<String,Long>> iterator = pages.entrySet().iterator() ;
		Map.Entry<String, Long> pagesEntry;
        while(iterator.hasNext()){
        	pagesEntry = iterator.next();
			context.write(new Text(pagesEntry.getKey().toString()), new Text(Long.toString(idvisit)));
		}

	}

}
