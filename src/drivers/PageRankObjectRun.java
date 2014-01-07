package drivers;

import mappers.PageRankObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import reducers.PageRankObjectReducer;
import utils.Node;

public class PageRankObjectRun extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(PageRankObjectRun.class);
		job.setMapperClass(PageRankObjectMapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Node.class);
		
		job.setReducerClass(PageRankObjectReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
