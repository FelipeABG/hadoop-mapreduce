package pr.puc.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] words = value.toString().split("\\s+");

    for (String word : words) {
      context.write(new Text(word), new IntWritable(1));
    }
  }
}
