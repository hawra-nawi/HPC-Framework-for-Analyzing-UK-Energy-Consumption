package org.myorg;

//Importing the required packages or libraries for the second Mapper class; it includes classes and methods 
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Using 'extend' to create a subclass called "Mapper1" from the Mapper parent class 
//Passing 4 parameters within this class
//1st and 2nd Parameter is the Input key and value datatype for the first Mapper class.
	// The input key is LongWritable as it is the document ID (of the temporarily output file from 1st MapReduce Job)
	// The input value is Text as it is the actual data inside each passing document ID
//3rd and 4th Parameter is output key and value datatypes from the first Mapper class
	// The output key is Text as this will be "1"
	// The output value is DoubleWritable as this the energy consumption value
public class Mapper2 extends Mapper <LongWritable, Text, Text, DoubleWritable>
{	
	//Text is equivalent to String in Java --> Use Hadoop's Object, Text, in order to use the Hadoop's objects and run the Mapper accordingly
	//Creating new Text Object called Year, which will be passed as the output Key of this Mapper class
	private Text Ones = new Text();
	private static DoubleWritable totalEnegry = new DoubleWritable(0);
	
	// Passing 3 parameters into this new map method
	// key, value, Context in order to execute and emit the context to the Reducer class
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException
	{
		// "line" String array is created
		// where it separates the values in the attributes within temporarily file's data.
		// separated by tab, as this structure format from the output temporarily file from the 1st MapReduce Job
		// Each values is stored in 1 array element
		String[] line = value.toString().split("\\t");
		
		// Creating String object to "1" 
		// as there is no unique keys, all the keys should be same in order for the 2nd reducer can only aggregate the values 
		String ones = "1";
		// Setting this new object, "ones" to the Key Text that will be passed and emit from the context output of this second mapper to the second reducer
		Ones.set(ones);
		
		// Will do the same for total energy
		// Create a "total" double variable that extracts the total sum energy values from the 2nd column (2nd from the line object) and parse is as Double
		double total = Double.parseDouble(line[1]);

		// Setting this new total variable to the Value DoubleWritable that will be passes 
		// and emit from the context output of this second mapper to the second reducer
		totalEnegry.set(total);

		// Lets now emit the Key and Value and Pass them to the second Reducer class
		context.write(Ones, totalEnegry);	
	}
}






