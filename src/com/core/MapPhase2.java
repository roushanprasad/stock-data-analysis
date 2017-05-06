package com.core;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapPhase2 extends Mapper<Text, Text, Text, Text>{

	@Override
	protected void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		if (value != null && !value.toString().isEmpty()) {
			
			try {
				String[] keyArr = key.toString().split("/");
				String stockId = keyArr[0];
				context.write(new Text(stockId), value);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	

}
