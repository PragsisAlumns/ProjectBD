package com.piwik.pairpagebyvisit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.piwik.common.RouteIdVisitWritable;


public class RouteVisitPageDriver extends Configured implements Tool {
	Logger logger = Logger.getLogger(RouteVisitPageDriver.class);
	
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
		job.setJarByClass(RouteVisitPageDriver.class);
		job.setJobName("Question 1: How many users are visiting page X and after page Y");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(RouteVisitPageMapper.class);
		job.setReducerClass(RouteVisitPageReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Setting map output 
		job.setMapOutputKeyClass(RouteIdVisitWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		//Setting combiner to reduce the amount of data in intermediate data
		job.setCombinerClass(RouteVisitPageCombiner.class);
		
		//Setting reduce output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//Setting partitioner
		job.setPartitionerClass(RouteVisitPagePartitioner.class);
		
		if (job.getCombinerClass() == null) {
		      throw new Exception("Combiner not set");
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new RouteVisitPageDriver(), args);
		System.exit(exitCode);
	}
}
