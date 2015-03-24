package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);
//		Configuration job_conf = context.getConfiguration();
//		System.out.println("!!!!!!!!"+job_conf.get("n"));
//		int n = Integer.parseInt(job_conf.get("n"));
		//int n =2;
		int n = Driver.n_gram;

		String[] temp = new String[n];
		int currentCount = 0;
		for (String word : words) {
			if(currentCount < n)
			{
				temp[currentCount] = word;
				currentCount++;
				if(currentCount < n) continue;
			}
			else
			{
				for(int j = 0; j < n-1; j++)
				{
					temp[j] = temp[j+1];
				}
				temp[n-1] = word;
			}
			String theNGram = "";
			for(int j = 0; j < n; j++)
			{
				theNGram = theNGram + temp[j] + " ";
			}
			//theNGram = "Hey";
			context.write(new Text(theNGram), NullWritable.get());
		}

	}
}
