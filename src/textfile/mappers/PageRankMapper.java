package textfile.mappers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import textfile.utils.Node;


/**
 * Mapper per il calcolo del PageRank senza nessun tipo di ottimizzazione.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class PageRankMapper extends Mapper<LongWritable, Text, LongWritable, Node> {

	private Double loss;
	private Integer cardinality;
	
	private Node node;
	private Node n_node;
	private LongWritable tK;
	private Text tmp;
	
	@Override
	protected void setup(Mapper<LongWritable,Text,LongWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		loss = new Double(0.0);
		cardinality = context.getConfiguration().getInt("cardinality", 1);
		
		node = new Node();
		n_node = new Node();
		tK = new LongWritable();
		tmp = new Text();
		
	};
	
	@Override
	protected void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

		node.clear();
		node.set(record);

		tK.set(Long.parseLong(node.getName().toString()));
		if (node.getPagerank() == -1) {
			node.setPagerank(1 / cardinality.doubleValue());
			node.setPagerankOld(1 / cardinality.doubleValue());
		}

		Double p = node.outputPageRank();

		node.setPagerankOld(node.getPagerank());
		node.setVertex(true);
		context.write(tK, node);
		
		if (node.getAdjacencyList().size() == 0) {
			loss += node.getPagerank();
		}
		
		for (String s : node.getAdjacencyList()) {			
			tK.set(Integer.parseInt(s));
			tmp.set(tK.toString());
			n_node.clear();
			n_node.setName(tmp);
			n_node.setPagerank(p);
			n_node.setVertex(false);
			context.write(tK, n_node);
		}
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, LongWritable, Node>.Context context) 	throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		Path filenamePath = new Path("OUTPUT/loss-tmp/" + UUID.randomUUID().toString());
		FSDataOutputStream out = fs.create(filenamePath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String o = new String("" + loss);

		bw.write(o);
		bw.close();

	};

}
