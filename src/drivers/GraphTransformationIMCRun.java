package drivers;

import mappers.GraphTransformationIMCMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;

import reducers.GraphTransformationIMCReducer;
import utils.Node;

/**
 * Classe driver per l'avvio dei job riguardanti la trasformazione del grafo e l'ordinamento.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class GraphTransformationIMCRun extends Configured implements Tool {

    Path partitionFile = new Path("OUTPUT/test_partitions.lst");
    Path outputStage = new Path("OUTPUT/test_staging");
    Path outputOrder = new Path("OUTPUT/pr-0.out");

	public int run(String[] args) throws Exception {
		
		
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "CardinalityStage");

//	INIZIO
	    // Configure job to prepare for sampling
		job.setJarByClass(GraphTransformationIMCRun.class);

	    // Use the mapper implementation with zero reduce tasks
		job.setMapperClass(GraphTransformationIMCMapper.class);
//		job.setReducerClass(CardinalityIMCReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[1]));

		job.setOutputKeyClass(LongWritable.class);
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
	        orderJob.setJarByClass(GraphTransformationIMCRun.class);

	        // Here, use the identity mapper to output the key/value pairs in
	        // the SequenceFile
	        orderJob.setMapperClass(Mapper.class);
//	        orderJob.setReducerClass(Reducer.class);
			orderJob.setReducerClass(GraphTransformationIMCReducer.class);


	        // Set the number of reduce tasks to an appropriate number for the
	        // amount of data being sorted
	        orderJob.setNumReduceTasks(Integer.parseInt(args[1]));

	        orderJob.setMapOutputKeyClass(LongWritable.class);
	        orderJob.setMapOutputValueClass(Node.class);
	        // Set the partition file

	        orderJob.setOutputKeyClass(LongWritable.class);
	        orderJob.setOutputValueClass(Node.class);

	        // Set the input to the previous job's output
	        orderJob.setInputFormatClass(SequenceFileInputFormat.class);
	        SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

	        // Set the output path to the command line parameter
//	        FileOutputFormat.setOutputPath(job, outputOrder);
	        orderJob.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(orderJob, outputOrder);
	        // Set the separator to an empty string
	        orderJob.getConfiguration().set("mapred.textoutputformat.separator", "\t");

	        // Use the InputSampler to go through the output of the previous
	        // job, sample it, and create the partition file
	        TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),partitionFile);

//	        InputSampler.writePartitionFile(job, new InputSampler.IntervalSampler<IntWritable, Node>(.001, 10000));
	        // Use Hadoop's TotalOrderPartitioner class
//	        InputSampler.writePartitionFile(job, new InputSampler.IntervalRandom(.001, 10000));
//	        InputSampler.writePartitionFile(job, new InputSampler.IntervalSampler<IntWritable, Node>(.001));
//	        InputSampler.writePartitionFile(job, new InputSampler.IntervalSampler<IntWritable, Node>(.001, 10000));
//	        InputSampler.writePartitionFile(job, new InputSampler.RandomSampler<IntWritable, Node>(.001, 10000));
	        InputSampler.writePartitionFile(orderJob,new InputSampler.SplitSampler<IntWritable, Node>(1000000));
	        orderJob.setPartitionerClass(TotalOrderPartitioner.class);

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
