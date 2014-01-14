package drivers;

import mappers.PageRankIMCMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import partitioners.OriginalOrderPartitioner;
import reducers.PageRankReducer;
import utils.Node;

/**
 * Classe driver per l'avvio del job riguardante il calcolo del pagerank con In-Map Combiner.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class PageRankIMCRun extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		Job job = Job.getInstance(conf , "PageRankIMCStage");
		
		job.setJarByClass(PageRankIMCRun.class);
		job.setMapperClass(PageRankIMCMapper.class);
		
		job.setReducerClass(PageRankReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Node.class);
		
		FileInputFormat.setInputPaths( job, new Path(args[0]) );

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(OriginalOrderPartitioner.class);
		
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
