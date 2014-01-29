package sequencefile.drivers;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import sequencefile.partitioners.OriginalOrderPartitioner;
import sequencefile.utils.Node;

public class FormatOutputRun extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "FormatOutput");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJarByClass(PageRankRun.class);
		job.setMapperClass(Mapper.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Node.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Node.class);

		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		job.setPartitionerClass(OriginalOrderPartitioner.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
