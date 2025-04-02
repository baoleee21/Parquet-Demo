import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

public class ParquetMapper extends Mapper<Void, Group, NullWritable, Group> {
    @Override
    protected void map(Void key, Group value, Context context) throws IOException, InterruptedException {
        // Chỉ đơn giản truyền dữ liệu ra output
        context.write(NullWritable.get(), value);
    }
}
