package sequencefile.drivers;

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
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;

import sequencefile.mappers.GraphTransformationIMCMapper;
import sequencefile.reducers.GraphTransformationIMCReducer;
import sequencefile.utils.Node;

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

		job.setJarByClass(GraphTransformationIMCRun.class);

		job.setMapperClass(GraphTransformationIMCMapper.class);

		job.setReducerClass(GraphTransformationIMCReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[1]));
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Node.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Node.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    SequenceFileOutputFormat.setOutputPath(job, outputStage);

	    int code = (job.waitForCompletion(true) ? 0 : 1);

	    if (code == 0) {
	    	
	        Job orderJob = new Job(conf, "TotalOrderSortingStage");
	        orderJob.setJarByClass(GraphTransformationIMCRun.class);

	        orderJob.setMapperClass(Mapper.class);
	        orderJob.setReducerClass(Reducer.class);

	        orderJob.setNumReduceTasks(Integer.parseInt(args[1]));

	        orderJob.setMapOutputKeyClass(LongWritable.class);
	        orderJob.setMapOutputValueClass(Node.class);

	        orderJob.setOutputKeyClass(LongWritable.class);
	        orderJob.setOutputValueClass(Node.class);

	        orderJob.setInputFormatClass(SequenceFileInputFormat.class);
	        SequenceFileInputFormat.setInputPaths(orderJob, outputStage);

	        orderJob.setOutputFormatClass(SequenceFileOutputFormat.class);
	        FileOutputFormat.setOutputPath(orderJob, outputOrder);

	        TotalOrderPartitioner.setPartitionFile(orderJob.getConfiguration(),partitionFile);

	        InputSampler.writePartitionFile(orderJob,new InputSampler.SplitSampler<IntWritable, Node>(1000000));
	        orderJob.setPartitionerClass(TotalOrderPartitioner.class);

	        code = orderJob.waitForCompletion(true) ? 0 : 2;
	    }

	    FileSystem.get(conf).delete(partitionFile, false);
	    FileSystem.get(conf).delete(outputStage, true);

		return code;
	}

}
