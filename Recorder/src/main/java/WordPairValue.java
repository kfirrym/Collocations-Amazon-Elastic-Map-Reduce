import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordPairValue implements Writable {

    private int firstWord;
    private int secondWord;
    private int pair;

    public WordPairValue(){}

    public WordPairValue(int firstWord, int secondWord, int pairOccurence) {
        this.firstWord = firstWord;
        this.secondWord = secondWord;
        this.pair = pairOccurence;
    }

    public int getFirstWord() { return firstWord; }

    public int getSecondWord() { return secondWord; }

    public int getPair() { return pair; }

    public void setFirstWord(int firstWord) { this.firstWord = firstWord; }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstWord = in.readInt();
        secondWord = in.readInt();
        pair = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(firstWord);
        out.writeInt(secondWord);
        out.writeInt(pair);
    }

    @Override
    public String toString() { return firstWord + ", " + secondWord + ", " + pair; }
}