package org.myorg;

//Importing the required packages or libraries for the first Mapper class; it includes classes and methods 
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Using 'extend' to create a subclass called "Mapper1" from the Mapper parent class 
//Passing 4 parameters within this class
//1st and 2nd Parameter is the Input key and value datatype for the first Mapper class.
	// The input key is LongWritable as it is the document ID
	// The input value is Text as it is the actual data inside each passing document ID
//3rd and 4th Parameter is output key and value datatypes from the first Mapper class
	// The output key is Text as this will be "year"
	// The output value is DoubleWritable as this the energy consumption value
public class Mapper1 extends Mapper <LongWritable, Text, Text, DoubleWritable>
{
	//Text is equivalent to String in Java --> Use Hadoop's Object, Text, in order to use the Hadoop's objects and run the Mapper accordingly
	//Creating new Text Object called Year, which will be passed as the output Key of this Mapper class
	private Text Year = new Text();
	// Same concept here; using Hadoop's Object, DoubleWritable, which is equivalent to Java's Object, Double
	// Creating new DoubleWritbale Object called totalEnergy, which will be passed as the output Value of this Mapper Class
	private static DoubleWritable totalEnegry = new DoubleWritable(0);
	
	// Overriding the map's original behaviour within the parent class
	@Override
	// Passing 3 parameters into this new map method
		// key, value, Context in order to execute and emit the context to the Reducer class
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
		// "line" String array is created
		// where it separates the values in the attributes within the dataset by ","
		// Each values is stored in 1 array element
		String[] line = value.toString().split(",");

		// Create a String called "year" to extract the year "values" from the forth column (Data Column); the 3 element of line
		// Using substring extract the year "values" with index starting from 6
		String year = line[3].substring(6);
		
		// Setting this new object, "year" to the Key Text that will be passed and emit from the context output of this first mapper to the first reducer
		Year.set(year);
		
		// Will do the same for total energy
		// Create a "total" double variable that extracts the total energy values from the 53th column (52th from the line object) and parse is as Double
		double total = Double.parseDouble(line[52].trim());

		// Setting this new total variable to the Value DoubleWritable that will be passes 
		// and emit from the context output of this first mapper to the first reducer
		totalEnegry.set(total);

		// Lets now emit the Key and Value and Pass them to the first Reducer class
		context.write(Year, totalEnegry);		
	}
}



