package textfile.partitioners;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import textfile.utils.Node;


/**
 * Partitioner che si occupa di mantenere l'ordine in output uguale a quello degli elementi letti in input nel/nei file/files.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */	
public class OriginalOrderPartitioner extends Partitioner<LongWritable, Node> implements Configurable {

	private Configuration configuration;
	
	@Override
	public int getPartition(LongWritable key, Node val, int numPartitions) {
		SchimmyPartitioner part = new SchimmyPartitioner();
		part.setConf(configuration);
		return part.getPartition(key, new DoubleWritable(), numPartitions);
	}

	@Override
	public Configuration getConf() {
		return configuration;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.configuration = arg0;
	}

}