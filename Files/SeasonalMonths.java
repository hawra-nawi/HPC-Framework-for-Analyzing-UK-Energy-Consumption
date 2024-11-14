package org.myorg;

//Importing the required packages or libraries for this driver; it includes classes and methods
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class SeasonalMonths

{
	public static void main(String[] args) throws Exception
	{
		// creating a new object from the "Configuration" class 
		// this "Configuration" allows this driver class to access the configuration parameters needed to perform the Hadoop's MapReduce Job
		Configuration conf = new Configuration();
		
		// Create an conditional 'if' statement to check and make sure the number of arguments is not greater than 3
		// the arguments are the Driver Class (jar file), followed by the input and output path when running the MapReduce Job within the cluster environment
		if (args.length != 3)
		{
			System.err.println("Usage: SeasonalMonths <input path> <output path>");
			// If the argument is greater than 3, than need to exist and stop Hadoop's cluster system from further performing the MapReduce Job
			System.exit(-1);
		}
		
		// Creating an object from the "Job" class 
		// This object is created to be used and represent the Hadoop's Job that will be executed
		// This is an important stage as it ensures that MapReduce Job will be executed, allowing users make adjustments or changes if needed
		Job job;
		job=Job.getInstance(conf, "Average of Each Seasonal Month");
		// Directing and allocating the Jar file to be the driver class
		job.setJarByClass(SeasonalMonths.class);

		// Setting up the input and output file path for the Hadoop's Job
		// remember that 1st argument is the driver class, that why input and output path is index with 1 and 2, respectively
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		//Calling and Setting up the Mapper and Reducer class 
		job.setMapperClass(SeasonalMonthsMapper.class);
		job.setReducerClass(SeasonalMonthsReducer.class);

		// Setting the Key and Value classes for the output from this Job
		// The key is Text as it is String of month per year ("month/year")
		job.setOutputKeyClass(Text.class);
		// The output is DoubleWritable as it is the energy consumption
		job.setOutputValueClass(DoubleWritable.class);
		
      	// Delete output if exists
		// This code below ensures that output folder within HDFS does not exist, and if it does exist, then it will be deleted in the HDFS system.
		// Either way, this functions ensures that new output folder is created within HDFS.
	   	FileSystem hdfs = FileSystem.get(conf);
	   	Path outputDir = new Path(args[2]);
	   	if (hdfs.exists(outputDir))
	   		hdfs.delete(outputDir, true);
	   	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}



