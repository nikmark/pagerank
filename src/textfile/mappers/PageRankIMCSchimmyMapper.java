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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper per il calcolo del PageRank, con In-Map Combiner e Schimmy design pattern.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */
public class PageRankIMCSchimmyMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {

	private Double loss;
	private Double pageRank;
	private Integer cardinality;
	private HashMap<LongWritable, Double> pageRankCollector;			
	
	@Override
	protected void setup(Mapper<LongWritable,Text,LongWritable,DoubleWritable>.Context context) throws IOException ,InterruptedException {
		
		loss = new Double(0.0);
		pageRank = new Double(0.0);
		cardinality = context.getConfiguration().getInt("cardinality", 0);
		pageRankCollector = new HashMap<LongWritable, Double>();	
		
	};
			
	@Override
	protected void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
		
		String[] split = record.toString().split("\\s");
				
		pageRank = Double.parseDouble(split[1]);
				
		if(pageRank == -1.0){
			pageRank = (1 / cardinality.doubleValue());
		}
				
		if(split.length == 2){
			loss += pageRank;
		}
							
		if(split.length > 2){
			for (int i = 2; i < split.length; i++) {				
				Double outputPageRank = pageRank / (split.length - 2);
				
				LongWritable childKey = new LongWritable(Integer.parseInt(split[i]));			
				pageRankCollector.put(childKey, 
						pageRankCollector.containsKey(childKey) ? (pageRankCollector.get(childKey) + outputPageRank) : outputPageRank);
			}
		}
				
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, LongWritable, DoubleWritable>.Context context) 	throws IOException, InterruptedException {
		
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
