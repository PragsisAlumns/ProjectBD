package com.piwik.routevisitpage;

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

import com.piwik.common.PageRouteWritable;


public class RouteVisitPageDriver extends Configured implements Tool {
	Logger logger = Logger.getLogger(RouteVisitPageDriver.class);
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: RouteVisitPageDriver <input dir> <output dir> \n");
			return -1;
		}
		
		//Create global variable
		Configuration conf=new Configuration();
		
		// Create the job
		Job job = Job.getInstance(getConf());
		job.setJarByClass(RouteVisitPageDriver.class);
		job.setJobName("Question 2: How many routes and visits by route are enabled to find page X ");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(RouteVisitPageMapper.class);
		job.setReducerClass(RouteVisitPageReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Setting map output 
		job.setMapOutputKeyClass(PageRouteWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//Setting reduce output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//Setting partitioner
		job.setPartitionerClass(RouteVisitPagePartitioner.class);
		
		//Setting combiner
		job.setCombinerClass(RouteVisitPageCombiner.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new RouteVisitPageDriver(), args);
		System.exit(exitCode);
	}
}
