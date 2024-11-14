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

public class TotalAverageEnergyConsumption{
	 
	public static void main(String[] args) throws Exception
	{
		/* 1st MapReduce Job */
		// creating a new object from the "Configuration" class 
		// this "Configuration" allows this driver class to access the configuration parameters needed to perform the Hadoop's MapReduce Chaining Jobs
		Configuration conf1 = new Configuration();
		
		// Create an conditional 'if' statement to check and make sure the number of arguments is not greater than 3
		// the arguments are the Driver Class (jar file), followed by the input and output path when running the MapReduce Job within the cluster environment
		if (args.length != 3)
		{
			System.err.println("Usage: TotalAverageEnergyConsumption <input path> <output path>");
			// If the argument is greater than 3, than need to exist and stop Hadoop's cluster system from further performing the first MapReduce Job 
			// of the MapReduce Chaining
			System.exit(-1);
		}
		
		// Creating an object from the "Job" class 
		// This object is created to be used and represent the Hadoop's MapReduce first Job that will be executed (to the second MapReduce Job)
		// This is an important stage as it ensures that this MapReduce Job will be executed, allowing users make adjustments or changes if needed
		Job job1;
		job1=Job.getInstance(conf1, "Total Sum Energy Consumption For each Year");
		// Directing and allocating the Jar file to be the driver class
		job1.setJarByClass(TotalAverageEnergyConsumption.class);
		
		//Calling and Setting up the Mapper1 and Reducer1 class (of the first MapReduce Job)
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		
		// Setting the Key and Value classes for the output from this Job
		// The key is Text as it is String of each year
		job1.setOutputKeyClass(Text.class);
		// The output is DoubleWritable as it is the energy consumption (the sum for each year) 
		job1.setOutputValueClass(DoubleWritable.class);
		
		// Setting up the input and output file path for the Hadoop's first MapReduce Job
		// remember that 1st argument is the driver class, that why input and output path is index with 1 and 2, respectively
		// However, the output path needs to be passed to the second MapReduce Job, therefore will add an file path called "temp", short for temporarily file path
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/temp"));
		
		// Delete output if exists
		// This code below ensures that output folder (for the temporarily file) within HDFS does not exist, 
		// and if it does exist, then it will be deleted in the HDFS system.
		// Either way, this functions ensures that new output folder (for the temporarily file path) is created within HDFS.
		FileSystem hdfs = FileSystem.get(conf1);
		Path outputDir = new Path(args[1]+"/temp");
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		// Completion of the 1st MapReduce becomes true when it finished and then we move onto the next chained MapReduce job
		job1.waitForCompletion(true);
			
		/* 2nd MapReduce Job */
		//Again, creating another new object from the "Configuration" class for configuration of this last part of the Hadoop's job
		Configuration conf2 = new Configuration();
		
		// Again, creating an object from the "Job" class 
		// This object is created to be used and represent the Hadoop's MapReduce second (and last) Job that will be executed (from the first MapReduce Job)
		Job job2;
		job2=Job.getInstance(conf2, "Total Average Energy Consumption");
		// Directing and allocating the Jar file to be the driver class
		job2.setJarByClass(TotalAverageEnergyConsumption.class);
		
		
		//Calling and Setting up the Mapper1 and Reducer1 class (of the second MapReduce Job)
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		
		// Setting the Key and Value classes for the output from this Job
		// The key is Text as it is Strings of "1"
		job2.setOutputKeyClass(Text.class);
		// The output is DoubleWritable as it is the energy consumption (the total average of all the years) 
		job2.setOutputValueClass(DoubleWritable.class);
		
		// the input path is the output path and temporarily file from the first MapReduce Job (hence the index being 1 plus temp file)
		// The output file is final output from this MapReduce Chaining, hence end with the last index (2)
		FileInputFormat.addInputPath(job2, new Path(args[1]+"/temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		// repeat the same process to check if there is a existing output file within HDFS
		hdfs = FileSystem.get(conf2);
		outputDir = new Path(args[2]);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
		// Completion of the 2nd MapReduce becomes true when it finished and therefore the MapReduce Chaining is completed successfully.
		System.exit(job2.waitForCompletion(true) ? 0 : 1);	
	}
}




