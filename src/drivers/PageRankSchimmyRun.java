package drivers;

import mappers.PageRankSchimmyMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import partitioners.SchimmyPartitioner;
import reducers.PageRankSchimmyReducer;

public class PageRankSchimmyRun extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf, "PageRankSchimmy");
		
		job.setJarByClass(PageRankSchimmyRun.class);
		job.setMapperClass(PageRankSchimmyMapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setPartitionerClass(SchimmyPartitioner.class);
//		job.setPartitionerClass(TotalOrderPartitioner.class);

//		job.setSortComparatorClass(IntWritable.Comparator.class);
		
		job.setReducerClass(PageRankSchimmyReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		
//		MultipleOutputs.addNamedOutput(job, "log", TextOutputFormat.class, Text.class, Text.class);
//
//	    MultipleOutputs.addNamedOutput(job, "output", TextOutputFormat.class, Text.class, Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
