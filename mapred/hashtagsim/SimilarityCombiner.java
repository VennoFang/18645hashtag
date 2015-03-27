package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

public class SimilarityCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	Log log = LogFactory.getLog(SimilarityCombiner.class);
	
    @Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			      Context context)
	throws IOException, InterruptedException {		
	    Integer count = 0;
		//log.info("[KEY]"+key);
	    for (IntWritable single : value) {
	    		count = count + single.get();
	    		//log.info("[VALUE_C]"+single.get());
	    }
	    
	    context.write(key, new IntWritable(count));
	    
        }
}
