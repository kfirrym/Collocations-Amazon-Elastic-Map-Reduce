import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordPairValue2 implements Writable {

    private int firstWord;
    private int secondWord;
    private int pair;
    private int total;

    public WordPairValue2(){}

    public WordPairValue2(int firstWord, int secondWord, int pairOccurence, int total) {
        this.firstWord = firstWord;
        this.secondWord = secondWord;
        this.pair = pairOccurence;
        this.total = total;
    }

    public int getFirstWord() { return firstWord; }

    public int getSecondWord() { return secondWord; }

    public int getPair() { return pair; }

    public int getTotal() { return total; }

    public void setFirstWord(int firstWord) { this.firstWord = firstWord; }

    public void setSecondWord(int secondWord) { this.secondWord = secondWord; }

    public void setTotal(int total) { this.total = total; }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstWord = in.readInt();
        secondWord = in.readInt();
        pair = in.readInt();
        total = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(firstWord);
        out.writeInt(secondWord);
        out.writeInt(pair);
        out.writeInt(total);
    }

    @Override
    public String toString() { return firstWord + ", " + secondWord + ", " + pair + ", " + total; }
}