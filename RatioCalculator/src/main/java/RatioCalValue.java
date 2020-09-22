import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RatioCalValue implements Writable, Comparable<RatioCalValue> {

    private String text;
    private double ratio;

    public RatioCalValue(){}

    public RatioCalValue(RatioCalValue other){
        this.text = other.getText();
        this.ratio = other.getRatio();
    }

    public RatioCalValue(String text, double ratio) {
        super();
        this.text = text;
        this.ratio = ratio;
    }

    public String getText() { return text; }

    public double getRatio() { return ratio; }

    @Override
    public void readFields(DataInput in) throws IOException {
        text = in.readUTF();
        ratio = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(text);
        out.writeDouble(ratio);
    }

    @Override
    public String toString() {
        return text + " : " + ratio;
    }

    @Override
    public int compareTo(RatioCalValue o) {
        double res = this.ratio - o.getRatio();
        return (res > 0) ? 1 : -1;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof RatioCalValue)
            return compareTo((RatioCalValue)obj) == 0;
        return false;
    }
}