package com.piwik.question1;

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
import org.junit.Before;
import org.junit.Test;

import com.piwik.common.RouteIdVisitWritable;
import com.piwik.pagepaths.PagesPathsMapper;
import com.piwik.pagepaths.PagesPathsReducer;
import com.piwik.routesbyvisit.RouteByIdVisitMapper;
import com.piwik.routesbyvisit.RouteByIdVisitReducer;

public class RouteByIdVisitTest {

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
		RouteByIdVisitMapper routeByIdVisitMapper = new RouteByIdVisitMapper();

		mapDriver = new MapDriver<LongWritable, Text, RouteIdVisitWritable, NullWritable>();
		mapDriver.setMapper(routeByIdVisitMapper);

		// Set up the reducer test harness.
		RouteByIdVisitReducer routeByIdVisitReducer = new RouteByIdVisitReducer();
		reduceDriver = new ReduceDriver<RouteIdVisitWritable, NullWritable, Text, LongWritable>();
		reduceDriver.setReducer(routeByIdVisitReducer);

		// Set up the mapper/reducer test harness.
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, RouteIdVisitWritable, NullWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(routeByIdVisitMapper);
		mapReduceDriver.setReducer(routeByIdVisitReducer);
		
	}

	/*
	 * Test the mapper.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMapper() throws IOException {
		
		//Given
		String lineHDFS = "1,23,2";
	
		//When		
		mapDriver.withInput(new LongWritable(1), new Text(lineHDFS));
		
		//Then
		mapDriver.withOutput(new RouteIdVisitWritable("23,2",1), NullWritable.get());
		
		mapDriver.runTest();

	
	}

	/*
	 * Test the reducer.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testOnlyTwoPagesReducer() throws IOException {
		/*
		//Given
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("0,2"));
		IntPairWritable key = new IntPairWritable(2,0);
				
		//When
		reduceDriver.withInput(key, values);

		//Then
		reduceDriver.withOutput(new LongWritable(key.getLeft()), new Text("0,2"));

		reduceDriver.runTest();
		*/
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testReducer() throws IOException {
		//Given
		List<NullWritable> values = new ArrayList<NullWritable>();
		values.add(NullWritable.get());
		values.add(NullWritable.get());
		values.add(NullWritable.get());
		Text key = new Text("2,3,567");
				
		//When
		reduceDriver.withInput(key, values);

		//Then
		reduceDriver.withOutput(new Text("2,3"), new LongWritable(567));

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
		RouteIdVisitWritable key = new RouteIdVisitWritable("23,2",1);

		//When
		mapReduceDriver.withInput(new LongWritable(1), new Text(lineHDFS));
		
		//Then
		mapReduceDriver.withOutput(key, NullWritable.get());
		
		mapReduceDriver.runTest();
	}
}

