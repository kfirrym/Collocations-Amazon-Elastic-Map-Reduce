import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Recorder {

    public static class MapperClass extends Mapper<DataGramKey, IntWritable,
            WordPairKey, WordPairValue> {

        @Override
        public void map(DataGramKey wordDecade, IntWritable count, Context context) throws IOException, InterruptedException {
            String[] words = wordDecade.getText().split(" ");
            if (words.length == 1) {
                context.write(new WordPairKey(words[0], "*", wordDecade.getDecade()),
                        new WordPairValue(count.get(), -1, -1));
                if(!words[0].equals("*"))
                    context.write(new WordPairKey("*", words[0], wordDecade.getDecade()),
                            new WordPairValue(-1, count.get(), -1));
            }else
                context.write(new WordPairKey(words[0], words[1], wordDecade.getDecade()),
                        new WordPairValue(-1, -1, count.get()));
        }
    }

    public static class ReducerClass extends Reducer<WordPairKey, WordPairValue,
            WordPairKey, WordPairValue> {

        private int wordOccurrence;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            wordOccurrence = 0;
        }

        @Override
        public void reduce(WordPairKey key, Iterable<WordPairValue> values, Context context) throws IOException,  InterruptedException {
            for (WordPairValue value : values)
                if(key.getSecond().equals("*") && !key.getFirst().equals("*"))
                    wordOccurrence = value.getFirstWord();
                else {
                    if(!key.getFirst().equals("*"))
                        value.setFirstWord(wordOccurrence);
                    context.write(key, value);
                }
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
        Job job = new Job(conf, "recorder");
        job.setJarByClass(Recorder.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setMapOutputKeyClass(WordPairKey.class);
        job.setMapOutputValueClass(WordPairValue.class);
        job.setOutputKeyClass(WordPairKey.class);
        job.setOutputValueClass(WordPairValue.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(DataGramInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
