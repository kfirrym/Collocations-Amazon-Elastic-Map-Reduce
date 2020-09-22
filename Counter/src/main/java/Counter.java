
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Counter {

    public static class MapperClass extends Mapper<LongWritable, Text,
            DataGramKey, IntWritable> {

        private ArrayList<String> stopWords;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            if(context.getConfiguration().get("lang", "heb").equals("heb"))
                stopWords = StopWords.getHeb();
            else
                stopWords = StopWords.getEng();
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] record = line.toString().split("\t");
            String[] words = record[0].split(" ");
            if(stopWords.contains(words[0]) || (words.length == 2 && stopWords.contains(words[1])))
                return;
            int decade = Integer.parseInt(record[1]) / 10;
            IntWritable amount = new IntWritable(Integer.parseInt(record[2]));
            context.write(new DataGramKey(record[0], decade), amount);
            if (words.length == 1)
                context.write(new DataGramKey("*", decade), amount);
        }
    }

    public static class ReducerClass extends Reducer<DataGramKey, IntWritable,
            DataGramKey, IntWritable> {
        @Override
        public void reduce(DataGramKey key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("lang", args[2]);
        Job job = new Job(conf, "counter");
        job.setJarByClass(Counter.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(DataGramKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(DataGramKey.class);
        job.setOutputValueClass(IntWritable.class);
        if(args[2].equals("heb")){
            FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));
            FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        }else{
            FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data"));
            FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data"));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
