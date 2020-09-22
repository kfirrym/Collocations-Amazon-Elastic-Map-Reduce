import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.TreeSet;


public class RatioCalculator {

    public static class MapperClass extends Mapper<WordPairKey, WordPairValue2,
            IntWritable, RatioCalValue> {

        private double L(double k, double n, double x){
            return Math.pow(x, k) * Math.pow(1 - x, n - k);
        }

        private double calculateRatio(double c1, double c2, double c12, double tw){
            double p, p1, p2;
            p = c2 / tw;
            p1 = c12 / c1;
            p2 = (c2 - c12) / (tw - c1);
            return Math.log(L(c12, c1, p)) + Math.log(L(c2 - c12, tw - c1, p)) -
                    Math.log(L(c12, c1, p1)) - Math.log(L(c2 - c12, tw - c1, p2));
        }

        @Override
        protected void map(WordPairKey key, WordPairValue2 value, Context context) throws IOException, InterruptedException {
            double ratio = calculateRatio(value.getFirstWord(),
                    value.getSecondWord(), value.getPair(), value.getTotal());
            if(!Double.isNaN(ratio))
                context.write(new IntWritable(key.getDecade() * 10),
                        new RatioCalValue(key.getFirst() + " " + key.getSecond(), ratio));
        }
    }

    public static class ReducerClass extends Reducer<IntWritable, RatioCalValue,
            IntWritable, RatioCalValue> {

        @Override
        public void reduce(IntWritable key, Iterable<RatioCalValue> values, Context context) throws IOException,  InterruptedException {
            TreeSet<RatioCalValue> stats = new TreeSet<>();
            for (RatioCalValue value : values) {
                stats.add(new RatioCalValue(value));
                if (stats.size() > 100)
                    stats.pollFirst();
            }
            for (RatioCalValue record : stats.descendingSet())
                context.write(key, record);
        }
    }

    public static class PartitionerClass extends Partitioner<IntWritable, RatioCalValue> {
        @Override
        public int getPartition(IntWritable key, RatioCalValue value, int numPartitions) {
            return key.get() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "counter");
        job.setJarByClass(RatioCalculator.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(RatioCalValue.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(RatioCalValue.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(WordPairInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
