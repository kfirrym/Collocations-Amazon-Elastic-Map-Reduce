import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Recorder2 {

    public static class MapperClass extends Mapper<WordPairKey, WordPairValue,
            WordPairKey, WordPairValue> { }

    public static class ReducerClass extends Reducer<WordPairKey, WordPairValue,
            WordPairKey, WordPairValue2> {

        private int wordOccurrence;
        private int totalWordOccurrence;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            wordOccurrence = 0;
            totalWordOccurrence = 0;
        }

        @Override
        public void reduce(WordPairKey key, Iterable<WordPairValue> values, Context context) throws IOException,  InterruptedException {
            for (WordPairValue value : values)
                if(key.getFirst().equals("*"))
                    if(key.getSecond().equals("*"))
                        totalWordOccurrence = value.getFirstWord();
                    else
                        wordOccurrence = value.getSecondWord();
                else
                    context.write(key, new WordPairValue2(value.getFirstWord(),
                            wordOccurrence, value.getPair(), totalWordOccurrence));
        }
    }

    public static class PartitionerClass extends Partitioner<WordPairKey, WordPairValue> {
        @Override
        public int getPartition(WordPairKey key, WordPairValue value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "recorder 2");
        job.setJarByClass(Recorder2.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setMapOutputKeyClass(WordPairKey.class);
        job.setMapOutputValueClass(WordPairValue.class);
        job.setOutputKeyClass(WordPairKey.class);
        job.setOutputValueClass(WordPairValue2.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(WordPairInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
