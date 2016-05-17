package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


@SuppressWarnings("unchecked")
public class FileCountPair implements Comparable, Writable {
  private String File;
  private int Count;

  public FileCountPair() {
    File = new String("");
    Count = 0;
  }

  public FileCountPair(FileCountPair a) {
    File = new String(a.File);
    Count = a.Count;
  }

  public FileCountPair(String File, int Count){
    this.File = File;
    this.Count = Count;
  }

  public String getFile() {
    return File;
  }

  public void setFile(String File) {
    this.File = File;
  }

  public int getCount() {
    return Count;
  }
  public void setCount(int Count) {
    this.Count = Count;
  }

  public int compareTo(Object anotherFileCountPair) throws ClassCastException {
    if (!(anotherFileCountPair instanceof FileCountPair))
      throw new ClassCastException("A FileCountPair Object expected");
    int anotherCount = ((FileCountPair) anotherFileCountPair).getCount();
    return -(this.Count - anotherCount);
  }
  public void readFields(DataInput in) throws IOException {
    this.File = in.readUTF();
    this.Count = in.readInt();
  }
  public void write(DataOutput out) throws IOException {
    out.writeUTF(File);
    out.writeInt(Count);
  }
}
