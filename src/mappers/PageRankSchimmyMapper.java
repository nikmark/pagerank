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

import utils.Data;

	
public class PageRankSchimmyMapper extends
		Mapper<LongWritable, Text, IntWritable, Data> {

	// Integer cardinality = new Integer(0);

	Double loss = new Double(0.0);
	Data data = new Data();

			
			
	@Override
	protected void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

		Integer cardinality = context.getConfiguration().getInt("cardinality", 0);

		loss = new Double(0.0);
		
		String[] split = record.toString().split("\\s");
		
		data = new Data();
		
		context.write(new IntWritable(Integer.parseInt(split[0])), data);
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
				Data tmp = new Data(data);
				tmp.setPageRank(tmp.getOutputPageRank(split.length - 2));
				context.write(new IntWritable(Integer.parseInt(split[i])), tmp);
			}
			
		}
				
	}

	protected void cleanup(Mapper<LongWritable, Text, IntWritable, Data>.Context context) 	throws IOException, InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());

		Path filenamePath = new Path("loss-tmp/" + UUID.randomUUID().toString());
		FSDataOutputStream out = fs.create(filenamePath);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

		String o = new String("" + loss);


		bw.write(o);
		bw.close();

	};

}
