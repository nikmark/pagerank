package textfile.mappers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper per il calcolo del PageRank senza nessun tipo di ottimizzazione.
 * 
 * @author Nicolò Marchi, Fabio Pettenuzzo
 *
 */	
public class PageRankSchimmyMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {

	private Integer cardinality;

	private Double loss;
	private Double pageRank;
	private Double output;
	
	private LongWritable newK;
	private DoubleWritable go;

	@Override
	protected void setup(Mapper<LongWritable,Text,LongWritable,DoubleWritable>.Context context) throws IOException ,InterruptedException {
		
		loss = new Double(0.0);
		pageRank = new Double(0.0);
		cardinality = context.getConfiguration().getInt("cardinality", 1);
		
		newK = new LongWritable();
		go = new DoubleWritable();
		
	};	
			
	@Override
	protected void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
		
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
			output = pageRank / (split.length-2);
			for (int i = 2; i < split.length; i++) {
				newK.set(Long.parseLong(split[i]));
				go.set(output);
				context.write(newK, go);

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
		System.out.println("perdita totale nei mapper: "+loss);


		bw.write(o);
		bw.close();

	};

}
