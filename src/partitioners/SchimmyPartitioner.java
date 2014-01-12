package partitioners;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import utils.Data;
import utils.Node;

public class SchimmyPartitioner extends Partitioner<IntWritable, Data> implements Configurable {

	HashMap<Integer, Integer[]> intervals;
    private Configuration configuration;
    int nodeCnt=0;
    
    public int getPartition(IntWritable key, Data data, int numReducers){
//    	NaturalKeyPartitioner n = new NaturalKeyPartitioner();
//    	n.getPartition(new IntWritable(Integer.parseInt(key.toString())), new Node(), numReducers);
    	return (int) (((float) key.get() / (float) (nodeCnt+1)) * numReducers) % numReducers;
    }

//	public int getPartition(IntWritable key, Data data, int numReducers) {
//		
//		intervals = populateIntervals(numReducers);
//		
//		for(Entry<Integer, Integer[]> e : intervals.entrySet()){
//			if( key.get() <= e.getValue()[1] && key.get() >= e.getValue()[0]){
//				return e.getKey();
//			}
//		}
//		return 0;
//	}
	
	private  HashMap<Integer, Integer[]> populateIntervals(int numReducers){
		
		HashMap<Integer, Integer[]> tmp  = new HashMap<Integer, Integer[]>();
		
		for(int i= 0; i<numReducers; i++){
			String[] split = configuration.get("interval-"+i).split("-");
			System.out.println(split.toString());
			tmp.put(i, new Integer[]{Integer.parseInt(split[0]), Integer.parseInt(split[1])});
		}
		
		return tmp;
	}

	@Override
	public Configuration getConf() {
		return configuration;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.configuration = arg0;
		nodeCnt = configuration.getInt("max", 1);
		
	}

}
