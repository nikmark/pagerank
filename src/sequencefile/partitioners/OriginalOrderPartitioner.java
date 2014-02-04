package sequencefile.partitioners;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner che si occupa di mantenere l'ordine in output uguale a quello degli elementi letti in input nel/nei file/files.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */	
public class OriginalOrderPartitioner extends Partitioner<LongWritable, Writable> implements Configurable {

	private Configuration configuration;
	private DoubleWritable m = new DoubleWritable();

	@Override
	public Configuration getConf() {
		return configuration;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.configuration = arg0;
	}

	@Override
	public int getPartition(LongWritable key, Writable val, int numPartitions) {
		
		SchimmyPartitioner part = new SchimmyPartitioner();
		part.setConf(configuration);
		return part.getPartition(key, m, numPartitions);
	}

}