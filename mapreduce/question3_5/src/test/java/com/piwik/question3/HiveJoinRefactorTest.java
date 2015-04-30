package com.piwik.question3;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.piwik.common.IntPairWritable;
import com.piwik.convertpage.ConvertPageCombiner;
import com.piwik.hivejoinrefactor.HiveJoinRefactorMapper;
import com.piwik.hivejoinrefactor.HiveJoinRefactorReducer;


public class HiveJoinRefactorTest {

	MapDriver mapDriver;
	ReduceDriver reduceDriver;
	MapReduceDriver mapReduceDriver;
	/*
	 * Set up the test. This method will be called before every test.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Before
	public void setUp() {
	
		// Set up the mapper test harness.
		HiveJoinRefactorMapper hiveJoinRefactorMapper = new HiveJoinRefactorMapper();

		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(hiveJoinRefactorMapper);

		// Set up the reducer test harness.
		HiveJoinRefactorReducer hiveJoinRefactorReducer = new HiveJoinRefactorReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(hiveJoinRefactorReducer);

		// Set up the mapper/reducer test harness.
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(hiveJoinRefactorMapper);
		mapReduceDriver.setReducer(hiveJoinRefactorReducer);
		
	
		
	}

	/*
	 * Test the mapper.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapper() throws IOException {
		
		//Given
		String lineHDFS1 = "2345,4,5#3,4,3,4";
		String lineHDFS2 = "252,5,4#3,4";
		
		//When		
		mapDriver.withInput(new LongWritable(1), new Text(lineHDFS1))
		.withInput(new LongWritable(1), new Text(lineHDFS2));
		
		//Then
		mapDriver.withOutput(new Text("23!5,4,5#3,4,3,4"), new Text("4"))
		.withOutput(new Text("2!2,5,4#3,4"), new Text("5"));
		
		mapDriver.runTest();
		
	}

	/*
	 * Test the reducer.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReducer() throws IOException {
		
		//Given
		Text key1 = new Text("23!5,4,5#3,4,3,4");
		Text key2 = new Text("2!2,5,4#3,4");
		List<Text> values1 = new ArrayList<Text>();
		List<Text> values2 = new ArrayList<Text>();
		values1.add(new Text("4,3"));
		values2.add(new Text("5"));
		
		//When
		reduceDriver.withInput(key1, values1)
		.withInput(key2,values2);
		
		//Then
		reduceDriver.withOutput(new Text("234,35,4,5#3,4,3,4"),new Text(" "))
		.withOutput(new Text("252,5,4#3,4"),new Text(" "));
		
		reduceDriver.runTest();
	}
	
	/*
	 * Test the mapper and reducer working together.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapReduce() throws IOException {
		
		//Given
		String lineHDFS1 = "2345,4,5#3,4,3,4";
		String lineHDFS2 = "252,5,4#3,4";
		String lineHDFS3 = "2335,4,5#3,4,3,4";
		
		//When
		mapReduceDriver.withInput(new LongWritable(1), new Text(lineHDFS1))
		.withInput(new LongWritable(2), new Text(lineHDFS2))
		.withInput(new LongWritable(3), new Text(lineHDFS3));
		
		//Then
		mapReduceDriver.withOutput(new Text("252,5,4#3,4"),new Text(" "))
		.withOutput(new Text("234,35,4,5#3,4,3,4"),new Text(" "));
		
		mapReduceDriver.runTest();

	}
}

