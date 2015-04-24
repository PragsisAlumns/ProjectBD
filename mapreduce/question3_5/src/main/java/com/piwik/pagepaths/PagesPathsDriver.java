package com.piwik.pagepaths;

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


public class PagesPathsDriver extends Configured implements Tool {
	Logger logger = Logger.getLogger(PagesPathsDriver.class);
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: CountPairPageDriver <input dir> <output dir> \n");
			return -1;
		}
		
		// Create the job
		Job job = Job.getInstance(getConf());
		job.setJarByClass(PagesPathsDriver.class);
		job.setJobName("Question 1: How many users are visiting page X and after page Y");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(PagesPathsMapper.class);
		job.setReducerClass(PagesPathsReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Setting map output 
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//Setting reduce output
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		/* 
		 * set sort comparator class so that idvisit/idss keys are
		 * sorted first in ascending order by name, then descending order by ids 
		 */
		job.setSortComparatorClass(IdVisitIdssComparator.class);
		
		/* 
		 * set the grouping comparator class so that all idvisit/idss keys
		 * with the same idvisit are grouped into the same call to the 
		 * reduce method
		 */
		job.setGroupingComparatorClass(IdVisitComparator.class);

		/*
		 * set custom partitioner so that string pair keys with the same 
		 * last idvisit go to the same reducer.
		 */
		job.setPartitionerClass(PairPagePartitioner.class);
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new PagesPathsDriver(), args);
		System.exit(exitCode);
	}
}
