import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataGramKey implements WritableComparable<DataGramKey> {

    private String text;
    private int decade;

    public DataGramKey(){}

    public DataGramKey(String text, int decade) {
        super();
        this.text = text;
        this.decade = decade;
    }

    public String getText() {
        return text;
    }

    public int getDecade() { return decade; }

    @Override
    public void readFields(DataInput in) throws IOException {
        text  = in.readUTF();
        decade = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(text);
        out.writeInt(decade);
    }

    @Override
    public int compareTo(DataGramKey other) {
        int result = this.decade - other.getDecade();
        if(result == 0)
            result = this.text.compareTo(other.getText());
        return result;
    }

    @Override
    public String toString() {
        return text + ", " + decade;
    }

}