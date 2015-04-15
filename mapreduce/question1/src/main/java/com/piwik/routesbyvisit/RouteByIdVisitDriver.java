package com.piwik.routesbyvisit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.piwik.common.IntPairWritable;


public class RouteByIdVisitDriver extends Configured implements Tool {
	Logger logger = Logger.getLogger(RouteByIdVisitDriver.class);
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: CountPairPageDriver <input dir> <output dir> \n");
			return -1;
		}
		
		//Create global variable
		Configuration conf=new Configuration();
		conf.setInt("countRoute", 0);
		
		// Create the job
		Job job = Job.getInstance(getConf());
		job.setJarByClass(RouteByIdVisitDriver.class);
		job.setJobName("Question 1: How many users are visiting page X and after page Y");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(RouteByIdVisitMapper.class);
		job.setReducerClass(RouteByIdVisitReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Setting map output 
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//Setting reduce output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		//Setting partitioner
		job.setPartitionerClass(RouteByIdVisitPartitioner.class);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new RouteByIdVisitDriver(), args);
		System.exit(exitCode);
	}
}
