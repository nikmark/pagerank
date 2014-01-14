package mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Node;

/**
 * Mapper per la trasformazione del grafo. Transforma il grafo dalla codifica del sito http://snap.stanford.edu ad una utile al job di calcolo del PageRank
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class GraphTransformationIMCMapper extends Mapper<LongWritable, Text, LongWritable, Node> {

	private Boolean cardinal;
	private HashMap<String, ArrayList<String>> format;
	
	protected void setup(Mapper<LongWritable,Text,LongWritable,Node>.Context context) throws IOException ,InterruptedException {
		
		cardinal = true;
		format = new HashMap<String, ArrayList<String>>();
		new Integer(0);
		
	};
	
	@Override
	public void map(LongWritable value, Text text, Context context)
			throws IOException, InterruptedException {
		
		if (!text.toString().startsWith("#")) {
			
			cardinal = false;

			String[] split = text.toString().split("\\s");
			if (format.containsKey(split[0])) {
				format.get(split[0]).add(split[1]);
			} else {
				format.put(split[0], new ArrayList<String>(Arrays.asList(split[1])));
			}

			if (!format.containsKey(split[1])) {
				format.put(split[1], new ArrayList<String>());
			}
		}
		

	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, LongWritable, Node>.Context context) 	throws IOException, InterruptedException {

		if (!cardinal) {

			Node n = new Node();

			for (Entry<String, ArrayList<String>> entry : format.entrySet()) {
				n = new Node();
				n.setName(new Text(entry.getKey()));
				n.setAdjacencyList(entry.getValue());

				context.write(new LongWritable(Integer.parseInt(entry.getKey())), n);
			}
		}
	};
}
