package com.piwik.hivejoinrefactor;

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

public class HiveJoinRefactorMapper extends Mapper<LongWritable, Text, Text, Text> {
	Logger logger = Logger.getLogger(HiveJoinRefactorMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//Splitting idvisit and paths 
		String[] fields = value.toString().split("");
		
		context.write(new Text(fields[0]+"!"+fields[2]), new Text(fields[1]));
		
	}
}
	