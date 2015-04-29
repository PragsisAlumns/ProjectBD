package com.piwik.question2;

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

import com.piwik.common.IntPairWritable;
import com.piwik.pagepaths.PagesPathsMapper;
import com.piwik.pagepaths.PagesPathsReducer;

public class PagesPathsTest {

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
		PagesPathsMapper countPairPageMapper = new PagesPathsMapper();

		mapDriver = new MapDriver<Text, Text, Text, Text>();
		mapDriver.setMapper(countPairPageMapper);

		// Set up the reducer test harness.
		PagesPathsReducer countPairPageReducer = new PagesPathsReducer();
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
		String lineHDFS = "1,23,2,0";
		IntPairWritable key = new IntPairWritable(23,1);
		String value = "0,2";
		
		//When		
		mapDriver.withInput(new LongWritable(1), new Text(lineHDFS));

		//Then
		mapDriver.withOutput(key, new Text(value));

		mapDriver.runTest();
		
	}

	/*
	 * Test the reducer.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testOnlyTwoPagesReducer() throws IOException {
		//Given
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("0,2"));
		IntPairWritable key = new IntPairWritable(2,0);
				
		//When
		reduceDriver.withInput(key, values);

		//Then
		reduceDriver.withOutput(new LongWritable(key.getLeft()), new Text("0,2"));

		reduceDriver.runTest();
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testReducer() throws IOException {
		//Given
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("0,2"));
		values.add(new Text("2,4"));
		values.add(new Text("4,2"));
		IntPairWritable key = new IntPairWritable(2,0);
				
		//When
		reduceDriver.withInput(key, values);

		//Then
		reduceDriver.withOutput(new LongWritable(key.getLeft()), new Text("0,2,4,2"));

		reduceDriver.runTest();
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testReducerWithSeveralPathsForTheSameVisit() throws IOException {
		//Given
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("0,2"));
		values.add(new Text("2,4"));
		values.add(new Text("4,2"));
		values.add(new Text("3,4"));
		values.add(new Text("4,2"));
		values.add(new Text("8,4"));
		IntPairWritable key = new IntPairWritable(2,0);
						
		//When
		reduceDriver.withInput(key, values);

		//Then
		reduceDriver.withOutput(new LongWritable(key.getLeft()), new Text("0,2,4,2#3,4,2#8,4"));

		reduceDriver.runTest();
		
	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMapReduce() throws IOException {
		
		//Given
		String lineHDFS = "1,23,2,0";
		IntPairWritable key = new IntPairWritable(23,1);
		String value = "0,2";
	
		//When
		mapReduceDriver.withInput(new LongWritable(1), new Text(lineHDFS));
		
		//Then
		mapReduceDriver.withOutput(new LongWritable(key.getLeft()), new Text(value));
		
		mapReduceDriver.runTest();
	}
}

