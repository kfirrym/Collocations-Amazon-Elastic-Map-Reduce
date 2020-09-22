import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class DataGramInputFormat extends FileInputFormat<DataGramKey, IntWritable> {

    public DataGramInputFormat() {
    }

    public RecordReader<DataGramKey, IntWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RecordReader<DataGramKey, IntWritable>() {

            protected LineRecordReader reader;
            protected DataGramKey key;
            protected IntWritable value;

            {
                reader = new LineRecordReader();
                key = null;
                value = null;
            }

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                reader.initialize(split, context);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (reader.nextKeyValue()) {
                    key = parseKey(reader.getCurrentValue().toString());
                    value = parseValue(reader.getCurrentValue().toString());
                    return true;
                } else {
                    key = null;
                    value = null;
                    return false;
                }
            }

            private DataGramKey parseKey(String str){
                String[] toks = str.split("\t")[0].split(", ");
                return new DataGramKey(toks[0], Integer.parseInt(toks[1]));
            }

            private IntWritable parseValue(String str){
                String[] toks = str.split("\t")[1].split(", ");
                return new IntWritable(Integer.parseInt(toks[0]));
            }

            @Override
            public DataGramKey getCurrentKey() throws IOException, InterruptedException {
                return key;
            }

            @Override
            public IntWritable getCurrentValue() throws IOException, InterruptedException {
                return value;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return reader.getProgress();
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = (new CompressionCodecFactory(context.getConfiguration())).getCodec(file);
        return null == codec || codec instanceof SplittableCompressionCodec;
    }
}
