package partitioners;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import utils.Data;

public class SchimmyPartitioner extends Partitioner<IntWritable, DoubleWritable> implements Configurable {

	HashMap<Integer, Integer[]> intervals = new HashMap<Integer, Integer[]>();
    private Configuration configuration;
    int nodeCnt=0;
    
//    public int getPartition(IntWritable key, Data data, int numReducers){
////    	NaturalKeyPartitioner n = new NaturalKeyPartitioner();
////    	n.getPartition(new IntWritable(Integer.parseInt(key.toString())), new Node(), numReducers);
//    	return (int) (((float) key.get() / (float) (nodeCnt+1)) * numReducers) % numReducers;
//    }
    
    

    @Override
	public int getPartition(IntWritable key, DoubleWritable data, int numReducers) {
		
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
//		nodeCnt = configuration.getInt("max", 1);
		nodeCnt = configuration.getInt("cardinality", 1);
		
	}

}
