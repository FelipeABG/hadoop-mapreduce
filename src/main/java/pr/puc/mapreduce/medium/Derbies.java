package pr.puc.mapreduce.medium;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import pr.puc.mapreduce.medium.value.DerbyStatsWritable;

public class Derbies extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    Integer result = ToolRunner.run(new Configuration(), new Derbies(), args);
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

    job.setJarByClass(Derbies.class);
    job.setMapperClass(DerbiesMapper.class);
    job.setReducerClass(DerbiesReducer.class);
    job.setCombinerClass(DerbiesCombiner.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DerbyStatsWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return 1;
    }
  }
}

class DerbiesMapper extends Mapper<LongWritable, Text, Text, DerbyStatsWritable> {
  protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
    String[] columns = value.toString().split(",");

    String homeState = columns[14];
    String visitorState = columns[15];

    if (homeState.equals(visitorState)) {

      Integer homeGoals = Integer.parseInt(columns[12]);
      Integer visitorGoals = Integer.parseInt(columns[13]);

      context.write(new Text(homeState), new DerbyStatsWritable(homeGoals + visitorGoals, 1));
    }

  }
}

class DerbiesCombiner extends Reducer<Text, DerbyStatsWritable, Text, DerbyStatsWritable> {
  protected void reduce(Text key, Iterable<DerbyStatsWritable> values, Context context)
      throws InterruptedException, IOException {

    Integer totalGoals = 0;
    Integer totalGames = 0;

    for (DerbyStatsWritable value : values) {
      totalGoals += value.getGoals();
      totalGames += value.getGames();
    }

    context.write(key, new DerbyStatsWritable(totalGoals, totalGames));

  }
}

class DerbiesReducer extends Reducer<Text, DerbyStatsWritable, Text, Text> {
  protected void reduce(Text key, Iterable<DerbyStatsWritable> values, Context context)
      throws InterruptedException, IOException {

    Integer totalGoals = 0;
    Integer totalGames = 0;

    for (DerbyStatsWritable value : values) {
      totalGoals += value.getGoals();
      totalGames += value.getGames();
    }

    DerbyStatsWritable result = new DerbyStatsWritable(totalGoals, totalGames);

    context.write(key, new Text(result.toString()));

  }
}
