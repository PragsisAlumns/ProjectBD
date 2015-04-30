package com.piwik.hivejoinrefactor;

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


public class HiveJoinRefactorDriver extends Configured implements Tool {
	Logger logger = Logger.getLogger(HiveJoinRefactorDriver.class);
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: HiveJoinRefactorDriver <input dir> <output dir> \n");
			return -1;
		}
		
		//Create global variable
		Configuration conf=new Configuration();
		
		// Create the job
		Job job = Job.getInstance(getConf());
		job.setJarByClass(HiveJoinRefactorDriver.class);
		job.setJobName("Question 3_5: Conversion pages by user visits id and path followed ");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(HiveJoinRefactorMapper.class);
		job.setReducerClass(HiveJoinRefactorReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Setting map output 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//Setting reduce output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new HiveJoinRefactorDriver(), args);
		System.exit(exitCode);
	}
}
