package sequencefile.drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import sequencefile.mappers.PageRankMapper;
import sequencefile.partitioners.OriginalOrderPartitioner;
import sequencefile.reducers.PageRankReducer;
import sequencefile.utils.Node;

/**
 * Classe driver per l'avvio del job riguardante il calcolo del pagerank senza nessuna ottimizzazione.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class PageRankRun extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "PageRankStage");

		job.setJarByClass(PageRankRun.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));
				
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Node.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Node.class);

		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(OriginalOrderPartitioner.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);

	}

}
