package mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Node;

public class CardinalityIMCMapper extends
		Mapper<LongWritable, Text, IntWritable, Node> {

//	Long partial;
	Boolean cardinal = true;
	HashMap<String, ArrayList<String>> format = new HashMap<String, ArrayList<String>>();
	Integer max = new Integer(0);

//protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,IntWritable,Node>.Context context) throws IOException ,InterruptedException {
//	
//	
//};

	@Override
	public void map(LongWritable value, Text text, Context context)
			throws IOException, InterruptedException {
		
		if (!text.toString().startsWith("#")) {
			
			cardinal = false;

			String[] split = text.toString().split("\\s");
			if (format.containsKey(split[0])) {
				format.get(split[0]).add(split[1]);
//				if(max <= Integer.parseInt(split[1])){
//					max = Integer.parseInt(split[1]);
//				}
			} else {
				format.put(split[0],
						new ArrayList<String>(Arrays.asList(split[1])));
				
//				if(max <= Integer.parseInt(split[0])){
//					max = Integer.parseInt(split[0]);
//				}
			}

			if (!format.containsKey(split[1])) {
				format.put(split[1], new ArrayList<String>());
//				if(max <= Integer.parseInt(split[1])){
//					max = Integer.parseInt(split[1]);
//				}
			}
		}
		

	}

	protected void cleanup(
			Mapper<LongWritable, Text, IntWritable, Node>.Context context)
			throws IOException, InterruptedException {

		if (!cardinal) {

//			partial += format.size();
			Node n = new Node();
//			n.setName(new Text("count"));
//			n.setPage_rank(partial.doubleValue());
//			context.write(new Text("count"), n);

			for (Entry<String, ArrayList<String>> entry : format.entrySet()) {
				n = new Node();
				n.setName(new Text(entry.getKey()));
				n.setAdjacencyList(entry.getValue());

				context.write(new IntWritable(Integer.parseInt(entry.getKey())), n);
			}
		}
		
//		Node n = new Node();
//		n.setPage_rank(new Double(max));
//		context.write(new IntWritable(-2), n);
	};
}
