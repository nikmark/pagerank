package sequencefile.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import sequencefile.mappers.PageRankIMCMapper;
import sequencefile.partitioners.OriginalOrderPartitioner;
import sequencefile.reducers.PageRankReducer;
import sequencefile.utils.Node;

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
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Node.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Node.class);
		
		SequenceFileInputFormat.setInputPaths( job, new Path(args[0]) );

		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(OriginalOrderPartitioner.class);
		
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
