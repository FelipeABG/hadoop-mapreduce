package pr.puc.mapreduce.medium.value;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class StadiumGoalsWritable implements Writable {

  private Integer homeGoals;
  private Integer visitorGoals;

  public StadiumGoalsWritable() {
  }

  public StadiumGoalsWritable(Integer home, Integer visitor) {
    this.homeGoals = home;
    this.visitorGoals = visitor;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    homeGoals = in.readInt();
    visitorGoals = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.homeGoals);
    out.writeInt(this.visitorGoals);
  }

  public Integer getTotal() {
    return this.homeGoals + this.visitorGoals;
  }

}
