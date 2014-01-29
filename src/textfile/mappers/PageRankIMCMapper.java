package textfile.mappers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import textfile.utils.Node;


/**
 * Mapper per il calcolo del PageRank, con In-Map Combiner.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class PageRankIMCMapper extends Mapper<LongWritable, Text, LongWritable, Node> {

	private Double loss;
	private HashMap<LongWritable, Double> pageRankCollector;
	private Long cardinality;
	private Node node;
	private Node n_node;
	
	private LongWritable tK;

	private Text tmp;

	protected void setup(Mapper<LongWritable,Text,LongWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		loss = new Double(0.0);
		pageRankCollector = new HashMap<LongWritable, Double>();
		cardinality = context.getConfiguration().getLong("cardinality", 1);
		
		node = new Node();
		n_node = new Node();
		tK = new LongWritable();
		tmp = new Text();

		
	};

	@Override
	protected void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
		
		node.clear();
		node.set(record);

		if (node.getPagerank() == -1) {
			node.setPagerank(1 / cardinality.doubleValue());
			node.setPagerankOld(1 / cardinality.doubleValue());
		}

		node.setVertex(true);	
		
		Double p = node.outputPageRank();

		node.setPagerankOld(node.getPagerank());
		tK.set(Long.parseLong(node.getName().toString()));
		context.write(tK, node);
		
		if (node.getAdjacencyList().size() == 0) {
			loss += node.getPagerank();
		}
		
		for (String s : node.getAdjacencyList()) {
			LongWritable childKey = new LongWritable(Integer.parseInt(s));			
			pageRankCollector.put(childKey, 
					pageRankCollector.containsKey(childKey) ? (pageRankCollector.get(childKey) + p) : p);
		}
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, LongWritable, Node>.Context context) 	throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		Path filenamePath = new Path("OUTPUT/loss-tmp/" + UUID.randomUUID().toString());
		FSDataOutputStream out = fs.create(filenamePath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String o = new String("" + loss);
		
		for(Entry<LongWritable, Double> e : pageRankCollector.entrySet()) {
			tK.set(e.getKey().get());
			tmp.set(tK.toString());
			n_node.clear();
			n_node.setName(tmp);
			n_node.setPagerank(e.getValue());
			n_node.setVertex(false);
			context.write(tK, n_node);
		}
		
		bw.write(o);
		bw.close();

	};

}
