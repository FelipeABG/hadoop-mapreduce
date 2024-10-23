package pr.puc.mapreduce.medium;

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
import pr.puc.mapreduce.medium.key.TeamsWritable;

public class Matchups extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    Integer result = ToolRunner.run(new Configuration(), new Matchups(), args);
    System.exit(result);
  }

  @Override
  public int run(String[] arg0) throws Exception {

    Path input = new Path("dataset-brasileirao.csv");
    Path output = new Path("output");

    Configuration cfg = this.getConf();
    Job job = Job.getInstance(cfg);

    FileSystem fs = FileSystem.get(cfg);
    fs.delete(output, true);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setJarByClass(Matchups.class);
    job.setMapperClass(MatchupsMapper.class);
    job.setReducerClass(MatchupsReducer.class);
    job.setCombinerClass(MatchupsCombiner.class);

    job.setMapOutputKeyClass(TeamsWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setNumReduceTasks(1);

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return 1;
    }

  }

}

class MatchupsMapper extends Mapper<LongWritable, Text, TeamsWritable, IntWritable> {
  protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

    String[] columns = value.toString().split(",");

    String homeTeam = columns[4];
    String visitorTeam = columns[5];

    context.write(new TeamsWritable(homeTeam, visitorTeam), new IntWritable(1));
  }
}

class MatchupsCombiner extends Reducer<TeamsWritable, IntWritable, TeamsWritable, IntWritable> {
  protected void reduce(TeamsWritable key, Iterable<IntWritable> values, Context context)
      throws InterruptedException, IOException {

    Integer accumulator = 0;

    for (IntWritable value : values) {
      accumulator += value.get();
    }

    context.write(key, new IntWritable(accumulator));
  }
}

class MatchupsReducer extends Reducer<TeamsWritable, IntWritable, Text, IntWritable> {
  protected void reduce(TeamsWritable key, Iterable<IntWritable> values, Context context)
      throws InterruptedException, IOException {

    Integer accumulator = 0;

    for (IntWritable value : values) {
      accumulator += value.get();
    }

    context.write(new Text(key.toString()), new IntWritable(accumulator));
  }
}
