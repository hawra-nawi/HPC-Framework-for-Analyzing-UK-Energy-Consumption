package org.myorg;

//Importing the required packages or libraries for the second Reducer class; it includes classes and methods 
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//The aim of this class to is load the output from the second mapper into here
//then through the list of all the values, extract each "year"
//get the total sum from the total energy column in correspondence to the "year" (by doing group by)
// Then go through the list of all the values, extracting all "1"s  and 
// aggregate and get total average of energy consumption of all the years (from the total sum of energy consumption, the second column) 

//Using 'extend' to create a subclass called "Reducer2" from the Reducer parent class 
//Passing 4 parameters within this class
//1st and 2nd Parameter is the Input key and value datatype for the Reducer class (from the Mapper2 Class).
	// The input key is Text as it is the "ones"
	// The input value is DoubleWritable as it is the total sum energy consumption values
//3rd and 4th Parameter is output key and value datatypes from the Mapper class
	// The output key is Text as this will be a message
	// The output value is DoubleWritable as this will be one aggregated value showing the total average of energy consumption value (of all the years)
	// The output key and value will be the final output of this MapReduce Chaining
public class Reducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	//Creating new Text Object called message, which will be passed as the output Key of this Reducer2 class
	private Text message = new Text();
	//"EnergyList" Double Arraylist is created	
	ArrayList<Double> EnergyList = new ArrayList<Double>();

	// Overriding the reduce's original behaviour within the parent class
	@Override 
	// Passing 3 parameters into this new map method
		//key, value, Context in order to execute and emit the context to the Reducer2 class
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException 
	{
		// creating "SumofEnergy" double variable and initialising it to 0
		double SumofTotalEnergy=0.0;

		// Creating a for loop to through and get all the total energy values (from EnergyList) that is passed to the reducer
		// each value of from the EnergyList is aggregated into the "SumofTotalEnergy" 
		for (DoubleWritable value : values)
		{
			EnergyList.add(value.get());
			SumofTotalEnergy = SumofTotalEnergy + value.get();

		}
		
		// get the size number of energy from the arraylist		
		int size = EnergyList.size();
		
		//the the average of total sum of energy is calculated (for all the values)
		double mean = SumofTotalEnergy/size;
		
		// Creating and setting a text message for the Key output 
		String txt = "The total average electricity consumption over the period is: ";
		message.set(txt);
		
		
		//Emit the key and value of the Reducer2 from the Context to the output file
		context.write(message, new DoubleWritable(mean));
	}
}



