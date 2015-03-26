package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class SimilarityCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			      Context context)
	throws IOException, InterruptedException {		
	    Integer count = 0;
	    for (IntWritable single : value) {
		Integer singleValue = single.get();
	        count = count + singleValue;
	    }
	    
	    context.write(key, new IntWritable(count));
	    
        }
}
