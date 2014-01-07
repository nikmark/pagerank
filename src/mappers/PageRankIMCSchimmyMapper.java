package mappers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.Data;

	
public class PageRankIMCSchimmyMapper extends
		Mapper<LongWritable, Text, IntWritable, Data> {

	// Integer cardinality = new Integer(0);

	Double loss = new Double(0.0);
	Data data = new Data();
	HashMap<IntWritable, Double> pageRankCollector = new HashMap<IntWritable, Double>();			
			
	@Override
	protected void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

		Integer cardinality = context.getConfiguration().getInt("cardinality", 0);

		loss = new Double(0.0);
		
		String[] split = record.toString().split("\\s");
		
		data = new Data();
		
		// context.write(new IntWritable(Integer.parseInt(split[0])), data); nello schimmy non serve
		data.setPageRank(Double.parseDouble(split[1]));
				
		if(data.getPageRank() == -1){
			data.setPageRank(1 / cardinality.doubleValue());
		}
		
		data.setPageRankOld(data.getPageRank());
		
		if(split.length == 2){
			loss += data.getPageRank();
		}
							
		if(split.length > 2){
			for (int i = 2; i < split.length; i++) {				
				Double outputPageRank = data.getOutputPageRank(split.length - 2);
				
				IntWritable childKey = new IntWritable(Integer.parseInt(split[i]));			
				pageRankCollector.put(childKey, 
						pageRankCollector.containsKey(childKey) ? (pageRankCollector.get(childKey) + outputPageRank) : outputPageRank);
			}
			
		}
				
	}

	protected void cleanup(Mapper<LongWritable, Text, IntWritable, Data>.Context context) 	throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		Path filenamePath = new Path("loss-tmp/" + UUID.randomUUID().toString());
		FSDataOutputStream out = fs.create(filenamePath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String o = new String("" + loss);

		for(Entry<IntWritable, Double> e : pageRankCollector.entrySet()) {
			Data tmp = new Data(data);
			tmp.setPageRank(e.getValue());
			context.write(e.getKey(), tmp);
		}

		bw.write(o);
		bw.close();

	};

}
