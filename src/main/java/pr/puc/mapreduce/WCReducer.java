package pr.puc.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  protected void reducer(Text key, Iterable<IntWritable> values, Context context) {
  }
}
