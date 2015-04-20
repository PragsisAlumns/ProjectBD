package com.piwik.pagepaths;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.piwik.common.IntPairWritable;

public class PagesPathsReducer extends Reducer<IntPairWritable, Text, LongWritable, Text> {

	
	@Override
	public void reduce(IntPairWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		
		//Counting the number of pages
		String lastPage = "";
		String paths="";
		String routes="";
		
		//For each couple of pages as a values 
		for (Text line : values){
			String[] pages = line.toString().split(",");
			
			if (lastPage.equals("")){ //for the first value
				paths = pages[0]+","+pages[1];
			} else { //if the last page from the previous value is the same that the first page in the current one
				if (lastPage.equals(pages[0])){
					paths = paths.concat(","+pages[1]);
				} else {//new path found, so to include as a new path
					paths = paths.concat("#"+pages[0]+","+pages[1]);
				}	
			}
			lastPage = pages[1];
		}
		
		context.write(new LongWritable(key.getLeft()), new Text(paths));
	}

	
}
