package com.piwik.question1;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.piwik.countpairs.CountPairPageByUserReducer;
import com.piwik.countpairs.CountPairPageMapper;

public class CountPairPageTest {

	MapDriver mapDriver;
	ReduceDriver reduceDriver;
	MapReduceDriver mapReduceDriver;
	/*
	 * Set up the test. This method will be called before every test.
	 */
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() {
	
		// Set up the mapper test harness.
		CountPairPageMapper countPairPageMapper = new CountPairPageMapper();

		mapDriver = new MapDriver<Text, Text, Text, Text>();
		mapDriver.setMapper(countPairPageMapper);

		// Set up the reducer test harness.
		CountPairPageByUserReducer countPairPageReducer = new CountPairPageByUserReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, LongWritable>();
		reduceDriver.setReducer(countPairPageReducer);

		// Set up the mapper/reducer test harness.
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, LongWritable, Text, Text, LongWritable>();
		mapReduceDriver.setMapper(countPairPageMapper);
		mapReduceDriver.setReducer(countPairPageReducer);
	
	}

	/*
	 * Test the mapper.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMapper() throws IOException {
		
		//Given
		String lineHDFS = "1,93 b1 03 34 8d 1e b4 07,2,0";
		String key = "0,2";
		String value = "93 b1 03 34 8d 1e b4 07_1";
		
		//When		
		mapDriver.withInput(new LongWritable(1), new Text(lineHDFS));

		//Then
		mapDriver.withOutput(new Text(key), new Text(value));

		mapDriver.runTest();
		
	}

	/*
	 * Test the reducer.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testReducer() throws IOException {
		
		//Given
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("93 b1 03 34 8d 1e b4 07_1"));
		values.add(new Text("93 b1 03 34 8d 1e b5 07_1"));
		values.add(new Text("93 b1 03 34 8d 1e b4 07_2"));
		String key = "2,0";
		
		//When
		reduceDriver.withInput(new Text(key), values);

		//Then
		reduceDriver.withOutput(new Text(key), new LongWritable(3));

		reduceDriver.runTest();
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testReducerWithRepitiedElements() throws IOException {
		
		//Given
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("93 b1 03 34 8d 1e b4 07_1"));
		values.add(new Text("93 b1 03 34 8d 1e b5 07_1"));
		values.add(new Text("93 b1 03 34 8d 1e b5 07_1"));
		values.add(new Text("93 b1 03 34 8d 1e b4 07_1"));
		values.add(new Text("93 b1 03 34 8d 1e b4 07_1"));
		String key = "2,0";
		
		//When
		reduceDriver.withInput(new Text(key), values);

		//Then
		reduceDriver.withOutput(new Text(key), new LongWritable(2));

		reduceDriver.runTest();
		
	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMapReduce() throws IOException {
		
		//Given
		String lineHDFS = "1,93 b1 03 34 8d 1e b4 07,2,0";
		String key = "0,2";
		String value = "93 b1 03 34 8d 1e b4 07_1";
		
		//When
		mapReduceDriver.withInput(new LongWritable(0), new Text(lineHDFS));
		
		//Then
		mapReduceDriver.addOutput(new Text(key), new LongWritable(1));
		
		mapReduceDriver.runTest();
		
	}
}
