package pr.puc.mapreduce.basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// The goal of this job is to determine the amount of games played on each stadium
public class GamesPerStadium extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GamesPerStadium(), args);

    System.exit(res);
  }

  @Override
  public int run(String[] arg0) throws Exception {

    Path input = new Path("dataset-brasileirao.csv");
    Path output = new Path("output/");

    Configuration cfg = this.getConf();
    Job job = Job.getInstance(cfg);

    FileSystem fs = FileSystem.get(cfg);
    fs.delete(output, true);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setJarByClass(GamesPerStadium.class);
    job.setMapperClass(GamesPerStadiumMapper.class);
    job.setReducerClass(GamesPerStadiumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);

    job.setNumReduceTasks(1);

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return 1;
    }

  }

}

class GamesPerStadiumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    String stadium = line.split(",")[11];

    context.write(new Text(stadium), new IntWritable(1));
  }

}

class GamesPerStadiumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  protected void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    int occurrances = 0;

    for (IntWritable value : values) {
      occurrances += value.get();
    }

    context.write(key, new IntWritable(occurrances));
  }
}
