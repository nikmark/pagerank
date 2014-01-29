package sequencefile.reducers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import sequencefile.utils.Node;

/**
 * Reducer per il calcolo del PageRank, che si occupa di recuperare tutte le parti del PageRank, calcolarne il nuovo valore, e valutare la convergenza.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */	
public class PageRankReducer extends Reducer<LongWritable, Node, LongWritable, Node> {
	
	private Double total_loss;
	private final Double ALPHA = new Double(0.85);
	private Integer cardinality;
	
	private Double percentage;

	private Node node; 
	
	@Override
	protected void setup(Reducer<LongWritable,Node,LongWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		total_loss = new Double(0.0);
		node = new Node();
		cardinality = new Integer(context.getConfiguration().getInt("cardinality", 1));
		percentage = new Double(context.getConfiguration().getDouble("percentage", 0.1));

		FileSystem fs = FileSystem.get(context.getConfiguration());
		RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("OUTPUT/loss-tmp"), true);
		
		while(list.hasNext()){
			
			FSDataInputStream out = fs.open(list.next().getPath());
			BufferedReader bw = new BufferedReader(new InputStreamReader(out));

			total_loss += Double.parseDouble(bw.readLine());
			bw.close();
		}
	};

	@Override
	protected void reduce(LongWritable key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
				
		Double sum = new Double(0.0);
		node.clear();
		
		for (Node n : values) {
			if (n.isVertex()) {
				node.set(n);
			} else {
				sum += n.getPagerank();
			}
		}
			
		//redistribuzione della massa persa
		sum = sum + (total_loss / cardinality.doubleValue());
		
		//calcolo del pagerank
		Double p = ((1 - ALPHA) / cardinality.doubleValue()) + ALPHA * sum;
		
		node.setPagerank(p);
		
		//Controllo di convergenza
		if(Math.abs(p-node.getPagerankOld()) > (percentage / cardinality.doubleValue())){
			FileSystem fs = FileSystem.get(context.getConfiguration());
			try {
				fs.createNewFile(new Path("OUTPUT/convergence"));
			} catch (Exception e) {}
		}
		
		context.write(key, node);
	}

}

