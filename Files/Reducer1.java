package org.myorg;

//Importing the required packages or libraries for the first Reducer class; it includes classes and methods 
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;

//The aim of this class to is load the output from the mapper into here
//then through the list of all the values, extract each "year"
//get the total sum from the total energy column in correspondence to the "year" (by doing group by)

//Using 'extend' to create a subclass called "Reducer1" from the Reducer parent class 
//Passing 4 parameters within this class
//1st and 2nd Parameter is the Input key and value datatype for the Reducer class (from the Mapper1 Class).
	// The input key is Text as it is the "year"
	// The input value is DoubleWritable as it is the total energy values
//3rd and 4th Parameter is output key and value datatypes from the Mapper class
	// The output key is Text as this will one aggregated "year" for each unique year
	// The output value is DoubleWritable as this will be 1 aggregated total sum energy consumption value 
	// The output key and value will be passed to Mapper2 (second Mapper of the 2nd MapReduce Job)

public class Reducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	//"EnergyList" Double Arraylist is created	
	ArrayList<Double> EnergyList = new ArrayList<Double>();
	
	// Overriding the reduce's original behaviour within the parent class
	@Override 
	// Passing 3 parameters into this new map method
		//key, value, Context in order to execute and emit the context to the Reducer1 class
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		// creating "SumofEnergy" double variable and initialising it to 0
		double SumofEnergy=0.0;
		
		// Creating a for loop to through and get all the total energy values (from EnergyList) that is passed to the reducer
		// each value of from the EnergyList is aggregated into the "SumofEnergy" 
		for (DoubleWritable value : values)
		{
			EnergyList.add(value.get());
			SumofEnergy = SumofEnergy + value.get();
		}
		// prints the total sum of energy consumption (of each year)
		System.out.println(SumofEnergy);
		
		//Emit the key and value of the Reducer from the Context to the output file
		// which will be passed onto the second MapReduce Job
		context.write(key, new DoubleWritable(SumofEnergy));
		}
}


