package partitioners;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import utils.Node;

public class NaturalKeyPartitioner extends Partitioner<IntWritable, Node>
		implements Configurable {

	// @Override
	// public int getPartition(IntWritable key, Node val, int numPartitions) {
	// int hash = key.hashCode();
	// int partition = hash % numPartitions;
	// return partition;
	// }

	private Configuration configuration;
	int nodeCnt = 0;

	public void maxKey() {
		FileSystem fs;
		try {
			fs = FileSystem.get(configuration);
			RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("max"), true);
			Integer max = new Integer(0);

			while (list.hasNext()) {

				FSDataInputStream out = FileSystem.get(configuration).open(list.next().getPath());
				BufferedReader bw = new BufferedReader(new InputStreamReader(out));

				max = Integer.parseInt(bw.readLine());
				if(nodeCnt <= max){
					nodeCnt = max;
				}
				bw.close();
				out.close();
			}
		} catch (Exception e) {}
		
		
		System.out.println("maxxxxx = "+nodeCnt);
	}

	@Override
	public Configuration getConf() {
		return configuration;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.configuration = arg0;
		// nodeCnt = configuration.getInt("max", 1);
	}

	@Override
	public int getPartition(IntWritable key, Node arg1, int numReducers) {
		return (int) (((float) key.get() / (float) nodeCnt) * numReducers) % numReducers;
	}

}