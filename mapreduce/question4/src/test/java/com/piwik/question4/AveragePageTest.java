package com.piwik.question4;

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

import com.piwik.averagepage.AveragePageCombiner;
import com.piwik.averagepage.AveragePageMapper;
import com.piwik.averagepage.AveragePagePartitioner;
import com.piwik.averagepage.AveragePageReducer;
import com.piwik.common.PageRouteWritable;


public class AveragePageTest {

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
		AveragePageMapper averagePageMapper = new AveragePageMapper();

		mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
		mapDriver.setMapper(averagePageMapper);

		// Set up the reducer test harness.
		AveragePageReducer averagePageReducer = new AveragePageReducer();
		reduceDriver = new ReduceDriver<Text, LongWritable, Text, Text>();
		reduceDriver.setReducer(averagePageReducer);

		// Set up the mapper/reducer test harness.
		AveragePageCombiner averagePageCombiner = new AveragePageCombiner();
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, PageRouteWritable, NullWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(averagePageMapper);
		mapReduceDriver.setReducer(averagePageReducer);
		mapReduceDriver.setCombiner(averagePageCombiner);
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
		String lineHDFS3 = "2355,4,5#3,4,3,4";
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(23,new LongWritable(7)));
		expectedOutput.add(new Pair(2,new LongWritable(5)));
		expectedOutput.add(new Pair(23,new LongWritable(7)));
		
		//When		
		mapDriver.withInput(new LongWritable(1), new Text(lineHDFS1))
		.withInput(new LongWritable(2), new Text(lineHDFS2))
		.withInput(new LongWritable(3), new Text(lineHDFS3));
		
		//Then		
		List<Pair> result = mapDriver.run();
		assertEquals(result.get(0).toString(),expectedOutput.get(0).toString());
		assertEquals(result.get(1).toString(),expectedOutput.get(1).toString());
		assertEquals(result.get(2).toString(),expectedOutput.get(2).toString());	
	}

	/*
	 * Test the reducer.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testAverageOneUserReducer() throws IOException {
		
		//Given
		Text key1 = new Text("21");

		List<LongWritable> values1 = new ArrayList<LongWritable>();
		values1.add(new LongWritable(7));
		values1.add(new LongWritable(7));
		values1.add(new LongWritable(7));
		
		//When
		reduceDriver.withInput(key1, values1);

		//Then
		reduceDriver.withOutput(new Text("Page visit average by converted user: "),new Text("7"));
		
		reduceDriver.runTest();
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReducer() throws IOException {

		//Given
		Text key1 = new Text("21");
		Text key2 = new Text("4");
		Text key3 = new Text("8");

		List<LongWritable> values1 = new ArrayList<LongWritable>();
		values1.add(new LongWritable(7));
		values1.add(new LongWritable(7));
		values1.add(new LongWritable(7));
				
		List<LongWritable> values2 = new ArrayList<LongWritable>();
		values2.add(new LongWritable(5));
		values2.add(new LongWritable(5));
		values2.add(new LongWritable(5));
		
		List<LongWritable> values3 = new ArrayList<LongWritable>();
		values3.add(new LongWritable(9));
		
		//When
		reduceDriver.withInput(key1, values1)
		.withInput(key2, values2)
		.withInput(key3, values3);
		
		//Then
		reduceDriver.withOutput(new Text("Page visit average by converted user: "),new Text("7"));
				
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
		String lineHDFS3 = "2355,4,5#3,4,3,4";
		
		//When
		mapReduceDriver.withInput(new LongWritable(1), new Text(lineHDFS1))
		.withInput(new LongWritable(2), new Text(lineHDFS2))
		.withInput(new LongWritable(3), new Text(lineHDFS3));
		
		//Then
		mapReduceDriver.withOutput(new Text("Page visit average by converted user: "),new Text("6"));

		mapReduceDriver.runTest();
	}
}

