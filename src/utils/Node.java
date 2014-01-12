package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.ID;

public class Node implements Writable {
	
	Text name = new Text("");
	Double pagerank = new Double(-1);	
	Double pagerankOld = new Double(-1);
	Boolean vertex = false;

	List<String> adjacencyList = new ArrayList<String>();
	
	public Node(Text record){
		
		String[] split = record.toString().split("\\s");
		name.set(split[0]);
		pagerank = Double.parseDouble(split[1]);
		pagerankOld = pagerank;
		
		for(int i=2; i< split.length; i++){
			adjacencyList.add(split[i]);
		}
		vertex = true;
	}
	
	public Node (Node old){
		this.name=old.getName();
		this.pagerank = old.getPagerank();
		this.pagerankOld = old.getPagerankOld();
		this.vertex=old.isVertex();
		this.adjacencyList = old.getAdjacencyList();
	}

	public Node() {
	}

	public Boolean isVertex() {
		return vertex;
	}

	public void setVertex(Boolean vertex) {
		this.vertex = vertex;
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public Double getPagerank() {
		return pagerank;
	}

	public void setPagerank(Double pagerank) {
		this.pagerank = pagerank;
	}

	public Double getPagerankOld() {
		return pagerankOld;
	}

	public void setPagerankOld(Double pagerankOld) {
		this.pagerankOld = pagerankOld;
	}

	public List<String> getAdjacencyList() {
		return adjacencyList;
	}

	public void setAdjacencyList(List<String> adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public Boolean getVertex() {
		return vertex;
	}

	public Double outputPageRank(){
		return pagerank / adjacencyList.size() ;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(pagerank);
		
		for(String s : adjacencyList){
			sb.append("\t").append(s);
		}
		
		return sb.toString();
	}
	

	@Override
	public void readFields(DataInput din) throws IOException {
		
		name.set(din.readUTF());
		pagerank = din.readDouble();
		pagerankOld = din.readDouble();
		vertex = din.readBoolean();
		
		int size = din.readInt();
		
		adjacencyList = new ArrayList<String>();
		
		for(int i = 0; i < size; i++){
			adjacencyList.add(din.readUTF());
		}
		
		
	}

	@Override
	public void write(DataOutput dou) throws IOException {

		dou.writeUTF(name.toString());
		dou.writeDouble(pagerank);
		dou.writeDouble(pagerankOld);
		dou.writeBoolean(vertex);
		
		dou.writeInt(adjacencyList.size());
		for(String s : adjacencyList){
			dou.writeUTF(s);
		}
		
	}
}
