package drivers;

import mappers.PageRankIMCSchimmyMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import partitioners.SchimmyPartitioner;
import reducers.PageRankSchimmyReducer;
import utils.Data;

public class PageRankObjectIMCSchimmyRun extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(PageRankObjectIMCSchimmyRun.class);
		job.setMapperClass(PageRankIMCSchimmyMapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Data.class);
		
		job.setPartitionerClass(SchimmyPartitioner.class); 
//		job.setPartitionerClass(TotalOrderPartitioner.class);

		job.setSortComparatorClass(IntWritable.Comparator.class);
		
		job.setReducerClass(PageRankSchimmyReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
