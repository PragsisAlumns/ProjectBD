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

import com.piwik.question1.CountPairPageByUserReducer;
import com.piwik.question1.CountPairPageMapper;

public class MostActiveTest {


	
	/*
	 * Set up the test. This method will be called before every test.
	 */
	@Before
	public void setUp() {
		/*EVERYTHING TO IMPLEMENT
		// Set up the mapper test harness.
		CountPairPageMapper distanceMapper = new CountPairPageMapper();

		reno = new BaseStation(1, 89521, "Reno", "NV", 39.5272F, 119.8219F);
		paloAlto = new BaseStation(1, 89521, "Palo Alto", "CA", 37.4292F, 122.1381F);
		distanceMapper.addBaseStation(reno);
		distanceMapper.addBaseStation(paloAlto);

		mapDriver = new MapDriver<LongWritable, Text, LongWritable, BaseStation>();
		mapDriver.setMapper(distanceMapper);

		// Set up the reducer test harness.
		CountPairPageByUserReducer activityReducer = new CountPairPageByUserReducer();
		reduceDriver = new ReduceDriver<LongWritable, BaseStation, Text, Text>();
		reduceDriver.setReducer(activityReducer);

		// Set up the mapper/reducer test harness.
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, LongWritable, BaseStation, Text, Text>();
		mapReduceDriver.setMapper(distanceMapper);
		mapReduceDriver.setReducer(activityReducer);
		*/
	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testMapper() throws IOException {
		/*EVERYTHING TO CHANGE
		DeviceData deviceData = new DeviceData(0, "name", "deviceId", 0, 0, 0, 0, 0, 0, "gpsStatus",
				"bluetoothStatus", "wifiStatus", 39.52F, 119.82F);
		mapDriver.withInput(new LongWritable(0), new Text(deviceData.toTabDelimitedString()));

		mapDriver.withOutput(new LongWritable(0), reno);

		mapDriver.runTest();
		*/
	}

	/*
	 * Test the reducer.
	 */
	@Test
	public void testReducer() throws IOException {
		/*EVERYTHING TO CHANGE
		List<BaseStation> values = new ArrayList<BaseStation>();
		values.add(reno);
		values.add(reno);

		reduceDriver.withInput(new LongWritable(0), values);

		reduceDriver.withOutput(new Text(dateFormat.format(new Date(0))), new Text("2\t" + reno.getId()));

		reduceDriver.runTest();
		*/
	}

	/*
	 * Test the mapper and reducer working together.
	 */
	@Test
	public void testMapReduce() throws IOException {
		/*EVERYTHING TO CHANGE
		DeviceData deviceData = new DeviceData(100, "name", "deviceId", 0, 0, 0, 0, 0, 0, "gpsStatus",
				"bluetoothStatus", "wifiStatus", 39.52F, 119.82F);
		mapReduceDriver.withInput(new LongWritable(1), new Text(deviceData.toTabDelimitedString()));
		
		mapReduceDriver.addOutput(new Text(dateFormat.format(new Date(0))), new Text("1\t" + reno.getId()));
		
		mapReduceDriver.runTest();
		*/
	}
}
