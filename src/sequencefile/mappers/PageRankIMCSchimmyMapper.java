package sequencefile.mappers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import sequencefile.utils.Node;

/**
 * Mapper per il calcolo del PageRank, con In-Map Combiner e Schimmy design pattern.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class PageRankIMCSchimmyMapper extends Mapper<LongWritable, Node, LongWritable, DoubleWritable> {

	private Double loss;
	private Double pageRank;
	private Double output;
	private Integer cardinality;
	private HashMap<LongWritable, Double> pageRankCollector;
	
	private Node node;	
	
	@Override
	protected void setup(Mapper<LongWritable,Node,LongWritable,DoubleWritable>.Context context) throws IOException ,InterruptedException {
		
		loss = new Double(0.0);
		pageRank = new Double(0.0);
		cardinality = context.getConfiguration().getInt("cardinality", 0);
		pageRankCollector = new HashMap<LongWritable, Double>();
		
		output  = new Double(0.0);
		node = new Node();
		
	};
			
	@Override
	protected void map(LongWritable key, Node record, Context context) throws IOException, InterruptedException {
		
		node.set(record);
		pageRank = node.getPagerank();
				
		if(pageRank == -1.0){
			pageRank = (1 / cardinality.doubleValue());
		}
				
		if(node.getAdjacencyList().size() == 0){
			loss += pageRank;
		}
		
		if(node.getAdjacencyList().size() > 0){
			output = pageRank / (node.getAdjacencyList().size());
			for(String s : node.getAdjacencyList()){				
				LongWritable childKey = new LongWritable(Long.parseLong(s));			
				pageRankCollector.put(childKey, 
						pageRankCollector.containsKey(childKey) ? (pageRankCollector.get(childKey) + output) : output);
			}
			
		}
				
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Node, LongWritable, DoubleWritable>.Context context) 	throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		Path filenamePath = new Path("OUTPUT/loss-tmp/" + UUID.randomUUID().toString());
		FSDataOutputStream out = fs.create(filenamePath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String o = new String("" + loss);

		for(Entry<LongWritable, Double> e : pageRankCollector.entrySet()) {
			context.write(e.getKey(), new DoubleWritable(e.getValue()));
		}

		bw.write(o);
		bw.close();

	};

}
