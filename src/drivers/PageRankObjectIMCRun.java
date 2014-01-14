package drivers;

import mappers.PageRankObjectIMCMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;

import reducers.PageRankObjectReducer;
import utils.Node;

public class PageRankObjectIMCRun extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
	    Path partitionFile = new Path("OUTPUT/test_partitions.lst");

		Job job = Job.getInstance(conf , "PageRankIMCStage");
		
		job.setJarByClass(PageRankObjectIMCRun.class);
		job.setMapperClass(PageRankObjectIMCMapper.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Node.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
        InputSampler.writePartitionFile(job,new InputSampler.SplitSampler(12));
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partitionFile);
        job.getConfiguration().set("mapred.textoutputformat.separator", "\t");

		job.setReducerClass(PageRankObjectReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));

//		FileInputFormat.se


//		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
