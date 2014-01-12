package drivers;

import mappers.CardinalityIMCMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import reducers.CardinalityIMCReducer;
import utils.Node;

public class CardinalityIMCRun extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
				
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CardinalityIMCRun.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(CardinalityIMCMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Node.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[1]));
		
		job.setReducerClass(CardinalityIMCReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("OUTPUT/pr-0.out"));

		return job.waitForCompletion(true) ? 0 : 1;
		
	}

}
