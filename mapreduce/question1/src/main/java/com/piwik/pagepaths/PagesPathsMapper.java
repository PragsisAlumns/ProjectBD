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


public class PagesPathsMapper extends Mapper<LongWritable, Text, IntPairWritable, Text> {
	Logger logger = Logger.getLogger(PagesPathsMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Splitting lines 
		String[] row = value.toString().split(",");
		
		//Selecting elements for the key IntPairWritable
		int idvisit = Integer.parseInt(row[1]);
		int idSs = Integer.parseInt(row[0]);
		
		//Selecting elements for value
		String fromPage = row[3];
		String toPage = row[2];
		
		
		// Emit pages couple 
		context.write(new IntPairWritable(idvisit,idSs), new Text(fromPage+","+toPage));
		
	}

}
