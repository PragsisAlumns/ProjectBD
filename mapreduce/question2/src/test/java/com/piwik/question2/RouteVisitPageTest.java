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

import com.piwik.common.PageRouteWritable;
import com.piwik.pagepaths.PagesPathsMapper;
import com.piwik.pagepaths.PagesPathsReducer;
import com.piwik.routevisitpage.RouteVisitPageCombiner;
import com.piwik.routevisitpage.RouteVisitPageMapper;
import com.piwik.routevisitpage.RouteVisitPagePartitioner;
import com.piwik.routevisitpage.RouteVisitPageReducer;

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

		mapDriver = new MapDriver<LongWritable, Text, PageRouteWritable, NullWritable>();
		mapDriver.setMapper(routeByIdVisitMapper);

		// Set up the reducer test harness.
		RouteVisitPageReducer routeByIdVisitReducer = new RouteVisitPageReducer();
		reduceDriver = new ReduceDriver<PageRouteWritable, NullWritable, Text, LongWritable>();
		reduceDriver.setReducer(routeByIdVisitReducer);

		// Set up the mapper/reducer test harness.
		RouteVisitPageCombiner routeVisitPageCombiner = new RouteVisitPageCombiner();
		RouteVisitPagePartitioner routevisitpagepartitioner = new RouteVisitPagePartitioner();
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, PageRouteWritable, NullWritable, Text, LongWritable>();
		mapReduceDriver.setMapper(routeByIdVisitMapper);
		mapReduceDriver.setReducer(routeByIdVisitReducer);
		mapReduceDriver.setCombiner(routeVisitPageCombiner);
	
		
	}

	/*
	 * Test the mapper.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapper() throws IOException {
		
		//Given
		String lineHDFS1 = "1	23,2,5,4,5#3,4,3,4";
		String lineHDFS2 = "2	2,5,4#3,4";
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(new PageRouteWritable("2","23,2"),new LongWritable(1)));
		expectedOutput.add(new Pair(new PageRouteWritable("5","23,2,5"),new LongWritable(1)));
		expectedOutput.add(new Pair(new PageRouteWritable("4","23,2,5,4"),new LongWritable(1)));
		expectedOutput.add(new Pair(new PageRouteWritable("5","23,2,5,4,5"),new LongWritable(1)));
		expectedOutput.add(new Pair(new PageRouteWritable("4","3,4"),new LongWritable(1)));
		expectedOutput.add(new Pair(new PageRouteWritable("3","3,4,3"),new LongWritable(1)));
		expectedOutput.add(new Pair(new PageRouteWritable("4","3,4,3,4"),new LongWritable(1)));
		expectedOutput.add(new Pair(new PageRouteWritable("5","2,5"),new LongWritable(1)));
		expectedOutput.add(new Pair(new PageRouteWritable("4","2,5,4"),new LongWritable(1)));
		expectedOutput.add(new Pair(new PageRouteWritable("4","3,4"),new LongWritable(1)));
		
		//When		
		mapDriver.withInput(new LongWritable(1), new Text(lineHDFS1))
		.withInput(new LongWritable(1), new Text(lineHDFS2));
		
		//Then		
		List<Pair> result = mapDriver.run();
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
	
	}

	/*
	 * Test the reducer.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testOnlyTwoPagesReducer() throws IOException {
		
		//Given
		PageRouteWritable key = new PageRouteWritable("0","2,0");
		List<LongWritable> values = new ArrayList<LongWritable>();
		values.add(new LongWritable(1));
		values.add(new LongWritable(2));
		values.add(new LongWritable(3));

		
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(new Text("0,1"),new LongWritable(6)));
				
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
		[((4,23,2,5,4), 1), ((5,23,2,5,4,5), 1), ((4,3,4), 1), ((3,3,4,3), 1), ((4,3,4,3,4), 1), ((5,2,5), 1), ((4,2,5,4), 1), ((4,3,4), 1)]
		PageRouteWritable key0 = new PageRouteWritable("4","3,4");
		PageRouteWritable key1 = new PageRouteWritable("2","23,2");
		PageRouteWritable key2 = new PageRouteWritable("5","23,2,5");
		
		List<LongWritable> values = new ArrayList<LongWritable>();
		values.add(new LongWritable(1));
		
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(new Text("0,3"),new LongWritable(9)));
		
		
		//When
		reduceDriver.withInput(key0, values)
		.withInput(key1,values)
		.withInput(key2,values);

		//Then
		List<Pair> result = reduceDriver.run();
		assertEquals(result.get(0).toString(),expectedOutput.get(0).toString());
		
	}
	
	/*
	 * Test the mapper and reducer working together.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapReduce() throws IOException {
		
		//Given
		String lineHDFS1 = "1	23,2,5,4,5#3,4,3,4";
		String lineHDFS2 = "2	2,5,4#3,4";
		
		List<NullWritable> values = new ArrayList<NullWritable>();
		values.add(NullWritable.get());
		values.add(NullWritable.get());
		
		List<Pair> expectedOutput = new ArrayList<Pair>();
		expectedOutput.add(new Pair(new Text("2,1"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("5,3"),new LongWritable(1)));
		expectedOutput.add(new Pair(new Text("4,3"),new LongWritable(4)));
		expectedOutput.add(new Pair(new Text("5,3,"),new LongWritable(3)));
		

		//When
		mapReduceDriver.withInput(new LongWritable(1), new Text(lineHDFS1))
		.withInput(new LongWritable(1), new Text(lineHDFS2));
		
		//Then
		List<Pair> result = mapReduceDriver.run();
		assertEquals(result.get(0).toString(),expectedOutput.get(0).toString());
		assertEquals(result.get(1).toString(),expectedOutput.get(1).toString());
		assertEquals(result.get(2).toString(),expectedOutput.get(2).toString());
		assertEquals(result.get(3).toString(),expectedOutput.get(3).toString());

	}
}

