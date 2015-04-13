package com.piwik.pagepaths;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;


public class PagesPathsMapper extends Mapper<LongWritable, Text, Text, Text> {
	Logger logger = Logger.getLogger(PagesPathsMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Select the page id "from" and "to"
		String[] row = value.toString().split(",");
		String idvisitor = row[1];
		String fromPage = row[3];
		String toPage = row[2];
		
		
		// Emit pages couple 
		context.write(new Text(idvisitor), new Text(fromPage+"_"+toPage));
		
	}

}
