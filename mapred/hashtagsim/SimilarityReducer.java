package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<Text, IntWritable, LongWritable, Text> {

	Log log = LogFactory.getLog(SimilarityReducer.class);
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context context)
			throws IOException, InterruptedException {		
		
		//log.info("[KEY]"+key);
	
		long count = 0;
		//Map<String, Integer> counts = new HashMap<String, Integer>();
		for (IntWritable single : value) {
			//log.info("[VALUE_R]"+single.get());	
			count+=single.get();
		}	
		
		
		
		/*
		 * We're serializing the word cooccurrence count as a string of the following form:
		 * 
		 * word1:count1;word2:count2;...;wordN:countN;
		 */
		
		
		
		if(count > 100000) context.write(new LongWritable(count), key);
	}
}
