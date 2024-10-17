package pr.puc.mapreduce.medium;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import pr.puc.mapreduce.medium.value.StadiumGoalsWritable;

public class AverageGoalsPerArena extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new Configuration(), new AverageGoalsPerArena(), args);
    System.exit(result);
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

    job.setJarByClass(AverageGoalsPerArena.class);
    job.setMapperClass(AvarageGoalsPerArenaMapper.class);
    job.setReducerClass(AvarageGoalsPerArenaReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(StadiumGoalsWritable.class);

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

class AvarageGoalsPerArenaMapper extends Mapper<LongWritable, Text, Text, StadiumGoalsWritable> {
  protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
    String[] line = value.toString().split(",");

    String stadium = line[11];
    Integer homeGoals = Integer.parseInt(line[12]);
    Integer visitorGoals = Integer.parseInt(line[13]);

    context.write(new Text(stadium), new StadiumGoalsWritable(homeGoals, visitorGoals));

  }
}

class AvarageGoalsPerArenaReducer extends Reducer<Text, StadiumGoalsWritable, Text, FloatWritable> {
  protected void reduce(Text key, Iterable<StadiumGoalsWritable> values, Context context)
      throws InterruptedException, IOException {

    Float occurances = 0f;
    Float total = 0f;

    for (StadiumGoalsWritable value : values) {
      total += value.getTotal();
      occurances += 1;
    }

    context.write(key, new FloatWritable(total / occurances));
  }
}
