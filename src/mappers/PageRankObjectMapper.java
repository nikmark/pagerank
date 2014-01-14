package mappers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Node;

public class PageRankObjectMapper extends Mapper<LongWritable, Text, IntWritable, Node> {

	Double loss = new Double(0.0);

	@Override
	protected void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

		Long cardinality = context.getConfiguration().getLong("cardinality", 1);

		Node node = new Node(record);

		if (node.getPagerank() == -1) {
			node.setPagerank(1 / cardinality.doubleValue());
			node.setPagerankOld(1 / cardinality.doubleValue());
		}

		Double p = node.outputPageRank();

		node.setPagerankOld(node.getPagerank());
		context.write(new IntWritable(Integer.parseInt(node.getName().toString())), node);
		
		if (node.getAdjacencyList().size() == 0) {
			loss += node.getPagerank();
		}
		
		for (String s : node.getAdjacencyList()) {
			Node n_node = new Node();
			n_node.setPagerank(p);
			n_node.setVertex(false);
			context.write(new IntWritable(Integer.parseInt(s)), n_node);
		}
	}

	protected void cleanup(Mapper<LongWritable, Text, IntWritable, Node>.Context context) 	throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		Path filenamePath = new Path("OUTPUT/loss-tmp/" + UUID.randomUUID().toString());
		FSDataOutputStream out = fs.create(filenamePath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String o = new String("" + loss);

		bw.write(o);
		bw.close();

	};

}
