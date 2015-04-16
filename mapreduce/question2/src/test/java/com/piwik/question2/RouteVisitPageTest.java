package com.piwik.question2;

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

import com.piwik.common.RouteIdVisitWritable;
import com.piwik.pagepaths.PagesPathsMapper;
import com.piwik.pagepaths.PagesPathsReducer;
import com.piwik.pairpagebyvisit.RouteVisitPageMapper;
import com.piwik.pairpagebyvisit.RouteVisitPageReducer;

public class RouteVisitPageTest {

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
		RouteVisitPageMapper routeByIdVisitMapper = new RouteVisitPageMapper();

		mapDriver = new MapDriver<LongWritable, Text, RouteIdVisitWritable, NullWritable>();
		mapDriver.setMapper(routeByIdVisitMapper);

		// Set up the reducer test harness.
		RouteVisitPageReducer routeByIdVisitReducer = new RouteVisitPageReducer();
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapper() throws IOException {
		
		//Given
		String lineHDFS = "1	23,2,5,4#3,4,3";
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(new RouteIdVisitWritable("23,2",1),NullWritable.get()));
		expectedOutput.add(new Pair(new RouteIdVisitWritable("23,5",1),NullWritable.get()));
		expectedOutput.add(new Pair(new RouteIdVisitWritable("23,4",1),NullWritable.get()));
		expectedOutput.add(new Pair(new RouteIdVisitWritable("2,5",1),NullWritable.get()));
		expectedOutput.add(new Pair(new RouteIdVisitWritable("2,4",1),NullWritable.get()));
		expectedOutput.add(new Pair(new RouteIdVisitWritable("5,4",1),NullWritable.get()));
		expectedOutput.add(new Pair(new RouteIdVisitWritable("3,4",1),NullWritable.get()));
		expectedOutput.add(new Pair(new RouteIdVisitWritable("3,3",1),NullWritable.get()));
		expectedOutput.add(new Pair(new RouteIdVisitWritable("4,3",1),NullWritable.get()));
		
		//When		
		mapDriver.withInput(new LongWritable(1), new Text(lineHDFS));
		
		//Then		
		List<Pair> result = mapDriver.run();
		assertEquals(result.get(0).toString(),expectedOutput.get(0).toString());
		assertEquals(result.get(1).toString(),expectedOutput.get(1).toString());
		assertEquals(result.get(2).toString(),expectedOutput.get(2).toString());
		assertEquals(result.get(3).toString(),expectedOutput.get(3).toString());
		assertEquals(result.get(4).toString(),expectedOutput.get(4).toString());
	
	}

	/*
	 * Test the reducer.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testOnlyTwoPagesReducer() throws IOException {
		
		//Given
		RouteIdVisitWritable key = new RouteIdVisitWritable("2,0",1);
		List<NullWritable> values = new ArrayList<NullWritable>();
		values.add(NullWritable.get());
		
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(new Text("2,0"),new LongWritable(1)));
				
		//When
		reduceDriver.withInput(key, values);

		//Then
		List<Pair> result = reduceDriver.run();
		assertEquals(result.get(0).toString(),expectedOutput.get(0).toString());
		
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReducer() throws IOException {

		//Given
		RouteIdVisitWritable key0 = new RouteIdVisitWritable("2,0",1);
		RouteIdVisitWritable key1 = new RouteIdVisitWritable("2,0",2);
		RouteIdVisitWritable key2 = new RouteIdVisitWritable("3,0",3);
		RouteIdVisitWritable key3 = new RouteIdVisitWritable("4,0",2);
		
		List<NullWritable> values = new ArrayList<NullWritable>();
		values.add(NullWritable.get());
		values.add(NullWritable.get());
		
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(new Text("2,0"),new LongWritable(2)));
		expectedOutput.add(new Pair(new Text("3,0"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("4,0"),new LongWritable(1)));
		
		//When
		reduceDriver.withInput(key0, values)
		.withInput(key1,values)
		.withInput(key2,values)
		.withInput(key3,values);

		//Then
		List<Pair> result = reduceDriver.run();
		assertEquals(result.get(0).toString(),expectedOutput.get(0).toString());
		assertEquals(result.get(1).toString(),expectedOutput.get(1).toString());
		assertEquals(result.get(2).toString(),expectedOutput.get(2).toString());
		
	}
	
	/*
	 * Test the mapper and reducer working together.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapReduce() throws IOException {
		
		//Given
		String lineHDFS1 = "1	23,2,5,4,2,5#3,4,3,4";
		String lineHDFS2 = "2	2,5,4#3,4";
		
		List<NullWritable> values = new ArrayList<NullWritable>();
		values.add(NullWritable.get());
		values.add(NullWritable.get());
		
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(new Text("2,2"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("2,4"),new LongWritable(2)));
		expectedOutput.add(new Pair(new Text("2,5"),new LongWritable(2)));
		expectedOutput.add(new Pair(new Text("23,2"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("23,4"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("23,5"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("3,3"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("3,4"),new LongWritable(2)));
		expectedOutput.add(new Pair(new Text("4,2"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("4,3"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("4,4"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("4,5"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("5,2"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("5,4"),new LongWritable(2)));
		expectedOutput.add(new Pair(new Text("5,5"),new LongWritable(1)));

		//When
		mapReduceDriver.withInput(new LongWritable(1), new Text(lineHDFS1))
		.withInput(new LongWritable(1), new Text(lineHDFS2));
		
		//Then
		List<Pair> result = mapReduceDriver.run();
		assertEquals(result.get(0).toString(),expectedOutput.get(0).toString());
		assertEquals(result.get(1).toString(),expectedOutput.get(1).toString());
		assertEquals(result.get(2).toString(),expectedOutput.get(2).toString());
		assertEquals(result.get(3).toString(),expectedOutput.get(3).toString());
		assertEquals(result.get(4).toString(),expectedOutput.get(4).toString());
		assertEquals(result.get(5).toString(),expectedOutput.get(5).toString());
		assertEquals(result.get(6).toString(),expectedOutput.get(6).toString());
		assertEquals(result.get(7).toString(),expectedOutput.get(7).toString());
		assertEquals(result.get(8).toString(),expectedOutput.get(8).toString());
		assertEquals(result.get(9).toString(),expectedOutput.get(9).toString());
		assertEquals(result.get(10).toString(),expectedOutput.get(10).toString());
		assertEquals(result.get(11).toString(),expectedOutput.get(11).toString());
		assertEquals(result.get(12).toString(),expectedOutput.get(12).toString());
		assertEquals(result.get(13).toString(),expectedOutput.get(13).toString());
		assertEquals(result.get(14).toString(),expectedOutput.get(14).toString());

	}
}

