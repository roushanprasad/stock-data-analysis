package com.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducePhase2 extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		List<Double> xiList = new ArrayList<Double>();
		Double xi = 0.0;
		Double xiSum = 0.0;
		Double numberOf = 0.0;
		Double xbar = 0.0;
		Double intermediateSum = 0.0;
		Double finalVolatility = 0.0;
		if (values != null && !values.toString().isEmpty()) {
			
			//Calculating the sum of xi values and 
			//number of xi values for calculating xbar
			for (Text text : values) {
				xi = Double.parseDouble(text.toString());
				xiList.add(xi);
				xiSum += xi;
				numberOf++;
			}
			xbar = (xiSum/numberOf);
			
			//Calculating the volatility using below formulae
			for (Double xiValue : xiList) {
				intermediateSum += Math.pow(xiValue - xbar, 2);
			}
			finalVolatility = Math.sqrt(intermediateSum/(numberOf-1));
			
			if (finalVolatility != null) {
				context.write(key, new Text(finalVolatility.toString()));
			}
		}
	}
	
	

}
