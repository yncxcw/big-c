package org.apache.hadoop.examples.kmeans;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class Cluster implements Writable{

	public long movie_id;
	public int total;
	public float similarity;
	public ArrayList<Review> reviews;
	
	public Cluster(){
		movie_id = -1;
		total = 0;
		similarity = 0.0f;
		reviews = new ArrayList<Review>();
	}
	public Cluster(Cluster a){ // copying the cluster
		movie_id = a.movie_id;
		total = a.total;
		similarity = a.similarity;
		reviews = new ArrayList<Review>();
		Review rv;
		for(int i = 0; i < a.reviews.size(); i++){
			rv = new Review(a.reviews.get(i)); 
			reviews.add(rv);
		}
	}
	public void readFields(DataInput in) throws IOException {
		movie_id = in.readLong();
		total = in.readInt();
		similarity = in.readFloat();
		reviews.clear();
		int size = in.readInt();
		Review rs; 
		for (int i = 0; i < size; i++){
			rs = new Review();
			rs.rater_id = in.readInt();
			rs.rating = in.readByte();
			reviews.add(rs);
		}
	}
	public void write(DataOutput out) throws IOException {
		out.writeLong(movie_id);
		out.writeInt(total);
		out.writeFloat(similarity);
		int size = reviews.size();
		out.writeInt(size);
		for(int i = 0; i < size; i++){
			out.writeInt(reviews.get(i).rater_id);
			out.writeByte(reviews.get(i).rating);
		}
	}
}
