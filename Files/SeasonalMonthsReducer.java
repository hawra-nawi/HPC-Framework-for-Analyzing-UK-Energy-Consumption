package org.myorg;

//Importing the required packages or libraries for the first Reducer class; it includes classes and methods 
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer;

//The aim of this class to is load the output from the mapper into here
// then through the list of all the values, extract the "month/year"
// get the average from the total column in correspondence to the "month/year" (by doing group by)

//Using 'extend' to create a subclass called "SeasonalMonthsReducer" from the Reducer parent class 
//Passing 4 parameters within this class
//1st and 2nd Parameter is the Input key and value datatype for the Reducer class (from the Mapper Class).
	// The input key is Text as it is the "month/year"
	// The input value is DoubleWritable as it is the total energy values
//3rd and 4th Parameter is output key and value datatypes from the Mapper class
	// The output key is Text as this will 1 aggregated "month/year"
	// The output value is DoubleWritable as this will be 1 aggregated average energy consumption value
public class SeasonalMonthsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	//"EnergyList" Double Arraylist is created
	ArrayList<Double> EnergyList = new ArrayList<Double>();
	
	// Overriding the reduce's original behaviour within the parent class
	@Override 
	// Passing 3 parameters into this new map method
		//key, value, Context in order to execute and emit the context to the Reducer class
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

		// get the size number of energy from the arraylist
		int size = EnergyList.size();
		
		//the the average energy energy is calculated (for each unique "month/year")
		double mean = SumofEnergy/size;

		//Emit the key and value of the Reducer1 from the Context to the output file
		context.write(key, new DoubleWritable(mean));
		}
}
	


	
	
	
	

