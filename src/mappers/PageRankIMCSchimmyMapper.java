package mappers;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	
public class PageRankIMCSchimmyMapper extends
		Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	// Integer cardinality = new Integer(0);

	Double loss = new Double(0.0);
	Double pageRank = new Double(0.0);
	HashMap<IntWritable, Double> pageRankCollector = new HashMap<IntWritable, Double>();			
			
	@Override
	protected void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

		Integer cardinality = context.getConfiguration().getInt("cardinality", 0);

//		loss = new Double(0.0);
		
		String[] split = record.toString().split("\\s");
		
//		data = new Data();
		
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
				
				IntWritable childKey = new IntWritable(Integer.parseInt(split[i]));			
				pageRankCollector.put(childKey, 
						pageRankCollector.containsKey(childKey) ? (pageRankCollector.get(childKey) + outputPageRank) : outputPageRank);
			}
//			for (int i = 2; i < split.length; i++) {				
//
//				Double outputPageRank = data.getOutputPageRank(split.length - 2);
//				
//				IntWritable childKey = new IntWritable(Integer.parseInt(split[i]));	
//				
//				if(pageRankCollector.containsKey(childKey)){
//					Data n = new Data(pageRankCollector.get(childKey));
//					n.setPageRank(n.getPageRank()+outputPageRank);
//					pageRankCollector.put(childKey, n);
//				}else{
//					Data n = new Data(data);
//					n.setPageRank(outputPageRank);
//					pageRankCollector.put(childKey, n);
//				}
//			}
		}
				
	}

	protected void cleanup(Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context) 	throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		Path filenamePath = new Path("OUTPUT/loss-tmp/" + UUID.randomUUID().toString());
		FSDataOutputStream out = fs.create(filenamePath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String o = new String("" + loss);

		for(Entry<IntWritable, Double> e : pageRankCollector.entrySet()) {
//			Data tmp = new Data();
//			tmp.setPageRank(e.getValue());
			System.out.println("chiave map: "+e.getKey().get()+" val map: "+e.getValue());
			context.write(e.getKey(), new DoubleWritable(e.getValue()));
		}

		bw.write(o);
		bw.close();

	};

}
