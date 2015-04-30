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
import com.piwik.convertpage.ConvertPageMapper;
import com.piwik.convertpage.ConvertPagePartitioner;
import com.piwik.convertpage.ConvertPageReducer;


public class ConvertPageTest {

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
		ConvertPageMapper convertPageMapper = new ConvertPageMapper();

		mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
		mapDriver.setMapper(convertPageMapper);

		// Set up the reducer test harness.
		ConvertPageReducer convertPageReducer = new ConvertPageReducer();
		reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
		reduceDriver.setReducer(convertPageReducer);

		// Set up the mapper/reducer test harness.
		ConvertPageCombiner convertPageCombiner = new ConvertPageCombiner();
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(convertPageMapper);
		mapReduceDriver.setReducer(convertPageReducer);
		mapReduceDriver.setCombiner(convertPageCombiner);
	
		
	}

	/*
	 * Test the mapper.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapper() throws IOException {
		
		//Given
		String lineHDFS1 = "234,35,4,5#3,4,3,4";
		String lineHDFS2 = "25,42,5,4#3,4";
		Text key0 = new Text("4");
		Text key1 = new Text("5");
		Text key2 = new Text("4");
		Text key3 = new Text("3");
		Text key4 = new Text("4");
		Text key5 = new Text("5");
		Text key6 = new Text("2");
		Text key7 = new Text("4");
		Text key8 = new Text("3");
		
		LongWritable value = new LongWritable(1);
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(key0,new LongWritable(1)));
		expectedOutput.add(new Pair(key1,new LongWritable(1)));
		expectedOutput.add(new Pair(key2,new LongWritable(1)));
		expectedOutput.add(new Pair(key3,new LongWritable(1)));
		expectedOutput.add(new Pair(key4,new LongWritable(1)));
		expectedOutput.add(new Pair(key5,new LongWritable(1)));
		expectedOutput.add(new Pair(key6,new LongWritable(1)));
		expectedOutput.add(new Pair(key7,new LongWritable(1)));
		expectedOutput.add(new Pair(key8,new LongWritable(1)));
		
		//When		
		mapDriver.withInput(new LongWritable(1), new Text(lineHDFS1))
		.withInput(new LongWritable(1), new Text(lineHDFS2));
		
		//Then		
		List<Pair> result = mapDriver.run();
		assertEquals(result.size(),9);
		assertEquals(result.get(0).toString(),expectedOutput.get(0).toString());
		assertEquals(result.get(1).toString(),expectedOutput.get(1).toString());
		assertEquals(result.get(2).toString(),expectedOutput.get(2).toString());
		assertEquals(result.get(3).toString(),expectedOutput.get(3).toString());
		assertEquals(result.get(4).toString(),expectedOutput.get(4).toString());
		assertEquals(result.get(5).toString(),expectedOutput.get(5).toString());
		assertEquals(result.get(6).toString(),expectedOutput.get(6).toString());
		assertEquals(result.get(7).toString(),expectedOutput.get(7).toString());
		assertEquals(result.get(8).toString(),expectedOutput.get(8).toString());
	}

	/*
	 * Test the reducer.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReducer() throws IOException {
		
		//Given
		Text key = new Text("0");
		List<LongWritable> values = new ArrayList<LongWritable>();
		values.add(new LongWritable(1));
		values.add(new LongWritable(2));
		values.add(new LongWritable(3));
				
		//When
		reduceDriver.withInput(key, values);
		
		//Then
		reduceDriver.withOutput(key,new LongWritable(6));
		
		reduceDriver.runTest();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSeveralInputsReducer() throws IOException {

		//Given
		Text key0 = new Text("0");
		Text key1 = new Text("1");
				
		List<LongWritable> values0 = new ArrayList<LongWritable>();
		values0.add(new LongWritable(1));
		values0.add(new LongWritable(2));

		List<LongWritable> values1 = new ArrayList<LongWritable>();
		values1.add(new LongWritable(1));
		values1.add(new LongWritable(2));
		values1.add(new LongWritable(3));
						
		//When
		reduceDriver.withInput(key0, values0)
		.withInput(key1,values1);
				
		//Then
		reduceDriver.withOutput(key0, new LongWritable(3))
		.withOutput(key1, new LongWritable(6));
				
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
		Text key0 = new Text("2");
		Text key1 = new Text("3");
		Text key2 = new Text("4");
		Text key3 = new Text("5");
		
		//When
		mapReduceDriver.withInput(new LongWritable(1), new Text(lineHDFS1))
		.withInput(new LongWritable(1), new Text(lineHDFS2));
		
		//Then
		mapReduceDriver.withOutput(key0, new LongWritable(1))
		.withOutput(key1, new LongWritable(1))
		.withOutput(key2, new LongWritable(2))
		.withOutput(key3, new LongWritable(2));
		
		mapReduceDriver.runTest();

	}
}

