package mappers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	
public class PageRankSchimmyMapper extends
		Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	// Integer cardinality = new Integer(0);

	Double loss = new Double(0.0);
	Double pageRank = new Double(0.0);

	//protected void setup(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,IntWritable,DoubleWritable>.Context context) throws IOException ,InterruptedException {
	//	loss = new Double(0.0)
	//};
			
			
	@Override
	protected void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {

		Integer cardinality = context.getConfiguration().getInt("cardinality", 0);

		//loss = new Double(0.0);
		
		String[] split = record.toString().split("\\s");
				
		pageRank = Double.parseDouble(split[1]);
				
		if(pageRank.equals(new Double(-1.0)) ){
			pageRank = (1 / cardinality.doubleValue());
		}
		
		if(split.length == 2){
			System.out.println("loss: "+loss);
			System.out.println("pr: "+pageRank);
			loss += pageRank;
			System.out.println("perdita: "+loss);
		}
							
		if(split.length > 2){
			for (int i = 2; i < split.length; i++) {
//				Data tmp = new Data();
//				tmp.setPageRank(tmp.getOutputPageRank(split.length - 2));
				Double output = pageRank / (split.length-2);
				context.write(new IntWritable(Integer.parseInt(split[i])),new DoubleWritable(output) );
			}
			
		}
				
	}

	protected void cleanup(Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context) 	throws IOException, InterruptedException {
		
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
