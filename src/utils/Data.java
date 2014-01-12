package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Data implements Writable {
	
	private Double pageRank = new Double(0);
	private Double pageRankOld = new Double(-1);
//	Double givenPageRank = new Double(0);
	
	public Data() {
	}	
	
	public Data(Data old) {
		this.pageRank = old.getPageRank();
		this.pageRankOld = old.getPageRankOld();
	}
	
	public Double getPageRank() {
		return pageRank;
	}
	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}
	public Double getPageRankOld() {
		return pageRankOld;
	}
	public void setPageRankOld(Double pageRankOld) {
		this.pageRankOld = pageRankOld;
	}
	
//	public Double getGivenPageRank() {
//		return givenPageRank;
//	}
//
//	public void setGivenPageRank(Double givenPageRank) {
//		this.givenPageRank = givenPageRank;
//	}

	public Double getOutputPageRank(int nNeighbours){
		return this.pageRank / nNeighbours;
	}
	@Override
	public void readFields(DataInput din) throws IOException {

		this.pageRank = din.readDouble();
		this.pageRankOld = din.readDouble();
		
	}
	@Override
	public void write(DataOutput dout) throws IOException {

		dout.writeDouble(pageRank);
		dout.writeDouble(pageRankOld);
		
	}


	
}
