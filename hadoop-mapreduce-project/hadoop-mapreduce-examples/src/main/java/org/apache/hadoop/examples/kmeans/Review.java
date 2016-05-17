package org.apache.hadoop.examples.kmeans;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Review implements Writable {

  public int rater_id;
  public byte rating;	

  public Review() {
    rater_id = -1;
    rating = 0;
  }

  public Review(Review a){
    rater_id = a.rater_id;
    rating = a.rating;
  }
  public void clear(){
    rater_id = -1;
    rating = 0;
  }
  public void readFields(DataInput in) throws IOException {
    rater_id = in.readInt();
    rating = in.readByte();
  }

  public void write (DataOutput out) throws IOException {
    out.writeInt(rater_id);
    out.writeByte(rating);
  }
}
