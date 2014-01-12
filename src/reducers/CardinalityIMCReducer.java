package reducers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Node;

public class CardinalityIMCReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
	
	Integer cardinality = new Integer(0);	
	Integer max = 0;

	
	protected void reduce(IntWritable key, java.lang.Iterable<Node> values, Reducer<IntWritable,Node,IntWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		sb.append(-1.0);
					
		Set<String> merged = new HashSet<String>();
		
		for(Node n : values){
			merged.addAll(n.getAdjacencyList());
		}
		
		Node node = new Node();
		node.setAdjacencyList(new ArrayList<String>(merged));

		cardinality++;
		
		if(max <= key.get()){
			max = key.get();
		}
		
		context.write(key, node);
		
	};
	
	protected void cleanup(Reducer<IntWritable,Node,IntWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		Path filenamePath = new Path("cardinality/"+UUID.randomUUID());
		FSDataOutputStream out = fs.create(filenamePath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String o = new String("" + cardinality + "\t" +max);
		
		bw.write(o);
		bw.close();

		
	};
}
