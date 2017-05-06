package com.core;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperPh3 extends Mapper<Text, Text, DoubleWritable, Text> {

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			context.write(new DoubleWritable(Double.parseDouble(value.toString())), key);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	

}
