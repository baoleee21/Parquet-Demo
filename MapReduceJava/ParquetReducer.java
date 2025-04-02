import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

public class ParquetReducer extends Reducer<NullWritable, Group, Void, Group> {
    @Override
    protected void reduce(NullWritable key, Iterable<Group> values, Context context) throws IOException, InterruptedException {
        for (Group value : values) {
            context.write(null, value);
        }
    }
}
