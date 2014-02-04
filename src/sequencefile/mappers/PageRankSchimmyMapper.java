package sequencefile.mappers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import sequencefile.utils.Node;

/**
 * Mapper per il calcolo del PageRank senza nessun tipo di ottimizzazione.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */	
public class PageRankSchimmyMapper extends Mapper<LongWritable, Node, LongWritable, DoubleWritable> {

	private Integer cardinality;

	private Double loss;
	private Double pageRank;
	private Double output;
	
	private LongWritable newK;
	private DoubleWritable go;
	
	private Node node;

	@Override
	protected void setup(Mapper<LongWritable,Node,LongWritable,DoubleWritable>.Context context) throws IOException ,InterruptedException {
		
		loss = new Double(0.0);
		pageRank = new Double(0.0);
		output = new Double(0.0);
		cardinality = context.getConfiguration().getInt("cardinality", 1);
		
		newK = new LongWritable();
		go = new DoubleWritable();
		
		node = new Node();
		
	};	
			
	@Override
	protected void map(LongWritable key, Node record, Context context) throws IOException, InterruptedException {
		
		node.set(record);
		pageRank = node.getPagerank();
				
		if(pageRank.equals(new Double(-1.0)) ){
			pageRank = (1 / cardinality.doubleValue());
		}
		
		if(node.getAdjacencyList().size() == 0){
			loss += pageRank;
		}
							
		if(node.getAdjacencyList().size() > 0){
			output = pageRank.doubleValue() / (node.getAdjacencyList().size());
			for(String s : node.getAdjacencyList()){
				newK.set(Long.parseLong(s));
				go.set(output);
				context.write(newK, go);
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
		System.out.println("perdita totale nei mapper: "+loss);


		bw.write(o);
		bw.close();

	};

}
