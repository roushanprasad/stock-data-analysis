package com.core;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperPh1 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//Getting file name without extension
		FileSplit fs = (FileSplit)context.getInputSplit();
		String fileName = fs.getPath().getName();
		String fNameWoExt = fileName.substring(0, fileName.lastIndexOf("."));
		
		//Extracting Month & Year from values to pass into key
		String[] facts = value.toString().split(",");
		if(facts != null && facts.length>0){
			String date = facts[0];
			System.out.println("Date is: "+date);
			
			
			if(!date.equals("Date")){
				String[] dateArr = date.split("-");
				String year = dateArr[0];
				System.out.println("Year is: "+year);
				String month = dateArr[1];
				System.out.println("Month is: "+month);
				
				Text mapKey = new Text(fNameWoExt+"/"+month+"/"+year);
				try {
					System.out.println("Key: "+mapKey+"\t"+"Value: "+value);
					context.write(mapKey, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}

		
}
