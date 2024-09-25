package pr.puc.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int occurrances = 0;

    for (IntWritable value : values) {
      occurrances += value.get();
    }

    context.write(key, new IntWritable(occurrances));
  }
}
