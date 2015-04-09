package com.piwik.question1;

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

/**
 * Reorganizes the data by stem
 */
public class CountPairPageMapper extends Mapper<Text, Text, Text, Text> {
	Logger logger = Logger.getLogger(CountPairPageMapper.class);
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		/*EVERYTHING TO IMPLEMENT

		// Emit the time and the closest BaseStation
		context.write(new LongWritable(calendar.getTimeInMillis()), closestBaseStation);
		*/
	}

}
