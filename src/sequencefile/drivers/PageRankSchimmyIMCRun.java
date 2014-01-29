package sequencefile.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import sequencefile.mappers.PageRankIMCSchimmyMapper;
import sequencefile.partitioners.SchimmyPartitioner;
import sequencefile.reducers.PageRankSchimmyReducer;
import sequencefile.utils.Node;

/**
 * Classe driver per l'avvio del job riguardante il calcolo del pagerank con In-Map Combiner e Schimmy design pattern.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class PageRankSchimmyIMCRun extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf, "PageRankIMCSchimmyStage");
		
		job.setJarByClass(PageRankSchimmyIMCRun.class);
		job.setMapperClass(PageRankIMCSchimmyMapper.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setPartitionerClass(SchimmyPartitioner.class); 

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Node.class);
		
		job.setReducerClass(PageRankSchimmyReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
