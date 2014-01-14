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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Node;

/**
 * Reducer per il mapper relativo alla trasformazione del grafo. Scrive su file il refactoring del grafo.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */	
public class GraphTransformationIMCReducer extends Reducer<LongWritable, Node, LongWritable, Node> {
	
	private Integer cardinality;	

	@Override
	protected void setup(Reducer<LongWritable,Node,LongWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		cardinality = new Integer(0);	
		
	};
	
	@Override
	protected void reduce(LongWritable key, java.lang.Iterable<Node> values, Reducer<LongWritable,Node,LongWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		sb.append(-1.0);
					
		Set<String> merged = new HashSet<String>();
		
		for(Node n : values){
			merged.addAll(n.getAdjacencyList());
		}
		
		Node node = new Node();
		node.setAdjacencyList(new ArrayList<String>(merged));

		cardinality++;
		
		context.write(key, node);
		
	};
	
	@Override
	protected void cleanup(Reducer<LongWritable,Node,LongWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		Path filenamePath = new Path("OUTPUT/cardinality/"+UUID.randomUUID());
		FSDataOutputStream out = fs.create(filenamePath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String o = new String("" + cardinality);
		
		bw.write(o);
		bw.close();

		
	};
}
