package reducers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Node;

public class PageRankObjectReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
	
	private Double total_loss = new Double(0.0);
	private final Double ALPHA = new Double(0.15);
	private Long cardinality = new Long(0);
	private Node node = new Node(); 
	
	protected void setup(Reducer<IntWritable,Node,IntWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		cardinality = context.getConfiguration().getLong("cardinality", 1);
		
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
	protected void reduce(IntWritable key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
		
		//System.out.println( "total loss data: " + total_loss);
		
		Double sum = new Double(0);
		
		for (Node n : values) {
				if (n.isVertex()) {
					node =new Node(n);
				} else {
					sum += n.getPagerank();
				}
			}
			
			//redistribuzione della massa persa
			sum = sum + (total_loss / cardinality);
			
			//calcolo del pagerank
			Double p = (ALPHA / cardinality) + (1-ALPHA)*sum;
			
			node.setPagerank(p);
			
			//Controllo di convergenza
			if(Math.abs(p-node.getPagerankOld()) > (0.1 / cardinality)){
				FileSystem fs = FileSystem.get(context.getConfiguration());
				try {
					fs.createNewFile(new Path("OUTPUT/convergence"));
				} catch (Exception e) {}
			}
			
			context.write(key, node);
	}

}

