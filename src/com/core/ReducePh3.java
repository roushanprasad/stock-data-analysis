package com.core;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducePh3 extends Reducer<DoubleWritable, Text, Text, Text>{

	@Override
	protected void reduce(DoubleWritable key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
		try {
			System.out.println("Hi Checking");
			for (Text text : value) {
				context.write(text, new Text(key.toString()));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

}
