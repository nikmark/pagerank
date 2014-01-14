package drivers;

import mappers.CardinalityIMCMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;

import reducers.CardinalityIMCReducer;
import start.Main;
import utils.Node;

public class CardinalityIMCRun extends Configured implements Tool {

	private int code;
    Path partitionFile = new Path("OUTPUT/test_partitions.lst");
    Path outputStage = new Path("OUTPUT/test_staging");
    Path outputOrder = new Path("OUTPUT/pr-0.out");

	public int run(String[] args) throws Exception {
		
		
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "CardinalityStage");
		
//		job.setJarByClass(CardinalityIMCRun.class);
//
//		job.setMapperClass(CardinalityIMCMapper.class);
//		job.setReducerClass(CardinalityIMCReducer.class);
//		job.setNumReduceTasks(Integer.parseInt(args[1]));
//		
//        job.setPartitionerClass(TotalOrderPartitioner.class);
//        
//        
//        job.setMapOutputKeyClass(IntWritableclass);
//        job.setMapOutputValueClass(Node.class);
//        // Set the partition file
//        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partitionFile);
//
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(Text.class);
//
//        // Set the input to the previous job's output
//        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
////        SequenceFileInputFormat.setInputPaths(job, new Path(args[0]));
//
//        // Set the output path to the command line parameter
////        FileOutputFormat.setOutputPath(job, outputOrder);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        FileOutputFormat.setOutputPath(job, outputOrder);
//        // Set the separator to an empty string
//        job.getConfiguration().set("mapred.textoutputformat.separator", "\t");
//
//        // Use the InputSampler to go through the output of the previous
//        // job, sample it, and create the partition file
//        InputSampler.writePartitionFile(job,new InputSampler.RandomSampler(.001, 10000, Integer.parseInt(args[1])));
//        
//        return job.waitForCompletion(true) ? 0 : 1;
        
        
        


//	    Path inputPath = new Path(args[0]);

//	INIZIO
	    // Configure job to prepare for sampling
		job.setJarByClass(CardinalityIMCRun.class);

	    // Use the mapper implementation with zero reduce tasks
		job.setMapperClass(CardinalityIMCMapper.class);
		job.setReducerClass(CardinalityIMCReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[1]));

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Node.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

	    // Set the output format to a sequence file
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    SequenceFileOutputFormat.setOutputPath(job, outputStage);

//	    .setOutputPath(sampleJob, outputStage); 	new Path("OUTPUT/pr-0.out")
	    int code = (job.waitForCompletion(true) ? 0 : 1);
	    // Submit the job and get completion code.
	    if (code == 0) {
	    	
	        Job orderJob = new Job(conf, "TotalOrderSortingStage");
	        orderJob.setJarByClass(CardinalityIMCRun.class);

	        // Here, use the identity mapper to output the key/value pairs in
	        // the SequenceFile
	        orderJob.setMapperClass(Mapper.class);
	        orderJob.setReducerClass(Reducer.class);
//			orderJob.setReducerClass(CardinalityIMCReducer.class);


	        // Set the number of reduce tasks to an appropriate number for the
	        // amount of data being sorted
	        orderJob.setNumReduceTasks(Integer.parseInt(args[1]));

	        // Use Hadoop's TotalOrderPartitioner class
	        orderJob.setPartitionerClass(TotalOrderPartitioner.class);

	        orderJob.setMapOutputKeyClass(IntWritable.class);
	        orderJob.setMapOutputValueClass(Node.class);
	        // Set the partition file
	        TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),partitionFile);

	        orderJob.setOutputKeyClass(IntWritable.class);
	        orderJob.setOutputValueClass(Node.class);

	        // Set the input to the previous job's output
	        orderJob.setInputFormatClass(SequenceFileInputFormat.class);
	        SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

	        // Set the output path to the command line parameter
//	        FileOutputFormat.setOutputPath(job, outputOrder);
	        orderJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(orderJob, outputOrder);
	        // Set the separator to an empty string
	        orderJob.getConfiguration().set("mapred.textoutputformat.separator", "\t");

	        // Use the InputSampler to go through the output of the previous
	        // job, sample it, and create the partition file
//	        InputSampler.writePartitionFile(job, new InputSampler.RandomSampler(.001, 10000, Integer.parseInt(args[1])));
	        InputSampler.writePartitionFile(orderJob,new InputSampler.SplitSampler(12));
	        // Submit the job
	        code = orderJob.waitForCompletion(true) ? 0 : 2;
	    }

	    // Clean up the partition file and the staging directory
	    FileSystem.get(new Configuration()).delete(partitionFile, false);
	    FileSystem.get(new Configuration()).delete(outputStage, true);

		return code;
//	FINE
	}

}
