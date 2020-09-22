import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordPairKey implements WritableComparable<WordPairKey> {

    private String first;
    private String second;
    private int decade;

    public WordPairKey(){}

    public WordPairKey(String first, String second, int decade) {
        super();
        this.first = first;
        this.second = second;
        this.decade = decade;
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }

    public int getDecade() { return decade; }

    @Override
    public void readFields(DataInput in) throws IOException {
        first  = in.readUTF();
        second = in.readUTF();
        decade = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeUTF(second);
        out.writeInt(decade);
    }

    @Override
    public int compareTo(WordPairKey other) {
        int result = this.decade - other.getDecade();
        if(result == 0)
            result = this.second.compareTo(other.getSecond());
        if(result == 0)
            result = this.first.compareTo(other.getFirst());
        return result;
    }

    @Override
    public String toString() {
        return first + ", " + second + ", " + decade;
    }

}