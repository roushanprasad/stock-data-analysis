package com.core;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducePh1 extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		int minDate= 99;
		int maxDate= -1;
		Double startDateAdjClosePrice = 0.0;
		Double endDateAdjClosePrice = 0.0;
		
		if(values != null && !values.toString().isEmpty()){
			for (Text line : values) {
				String[] lineArr = line.toString().split(",");
				String incomingDate = lineArr[0];
				
				//Getting Day from Date
				String[] dateArr = incomingDate.split("-");
				String day = dateArr[2];
				
				if(Integer.parseInt(day)< minDate ){
					minDate = Integer.parseInt(day);
					startDateAdjClosePrice = Double.parseDouble(lineArr[6]);
					System.out.println("For Stock: "+key+" ,Values are MinDate: "+minDate+", Start Date Adj Close Price: "+startDateAdjClosePrice);
				}
				if(Integer.parseInt(day)>maxDate){
					maxDate = Integer.parseInt(day);
					endDateAdjClosePrice = Double.parseDouble(lineArr[6]);
					System.out.println("For Stock: "+key+" ,Values are MaxDate: "+maxDate+", End Date Adj Close Price: "+endDateAdjClosePrice);
				}
				Double avg = (endDateAdjClosePrice - startDateAdjClosePrice)/startDateAdjClosePrice;
				String xi = avg.toString();
				System.out.println("For Stock: "+key+", Xi Value is: "+xi);
				Text reduceValue = new Text(xi);
				context.write(key, reduceValue);
			}
		}
		
	}
	
	

}
