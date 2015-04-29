package com.piwik.pairpagebyvisit;

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


public class PairPageByVisitMapper extends Mapper<LongWritable, Text, RouteIdVisitWritable, NullWritable> {
	Logger logger = Logger.getLogger(PairPageByVisitMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Splitting idvisit and paths 
		String[] fields = value.toString().split("\t");
		
		//Id visit 
		long idvisit = Long.parseLong(fields[0]);

		//Paths
		String[] singlePaths = fields[1].split("#");
		String pairPage;
		
		for (String path : singlePaths ){ //for each path
			String[] pages = path.split(","); //Split by pages
			for (int i=0; i<pages.length-1; i++){
				pairPage = "";
				for (int f=i+1; f<pages.length; f++){
					pairPage = pages[i]+","+pages[f];
					// Emit pages couple 
					context.write(new RouteIdVisitWritable(pairPage,idvisit), NullWritable.get());
					pairPage = "";
				}
			}
		}
	}

}
