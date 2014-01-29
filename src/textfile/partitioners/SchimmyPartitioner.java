package textfile.partitioners;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner per lo Schimmy design pattern. Detto anche RangePartitioner.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */	
public class SchimmyPartitioner extends Partitioner<LongWritable, DoubleWritable> implements Configurable {

	private HashMap<Integer, Integer[]> intervals = new HashMap<Integer, Integer[]>();
    private Configuration configuration;
    private int nodeCnt=0;

    @Override
	public int getPartition(LongWritable key, DoubleWritable data, int numReducers) {
		
		intervals = populateIntervals(numReducers);
		
		for(Entry<Integer, Integer[]> e : intervals.entrySet()){
			if( key.get() <= e.getValue()[1] && key.get() >= e.getValue()[0]){
				System.out.println("chiave: "+key.get()+" partition: "+e.getKey());
				return e.getKey();
			}
		}
		return 0;
	}
	
	private  HashMap<Integer, Integer[]> populateIntervals(int numReducers){
		
		HashMap<Integer, Integer[]> tmp  = new HashMap<Integer, Integer[]>();
		
		if(intervals.size() == 0){
			for(int i= 0; i<numReducers; i++){
				String[] split = configuration.get("interval-"+i).split("-");
				System.err.println(configuration.get("interval-"+i));
				tmp.put(i, new Integer[]{Integer.parseInt(split[0]), Integer.parseInt(split[1])});
			}
			return tmp;
		}
		
		return intervals;
	}

	@Override
	public Configuration getConf() {
		return configuration;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.configuration = arg0;
		nodeCnt = configuration.getInt("cardinality", 1);
		
	}

}
