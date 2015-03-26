package mapred.hashtagsim;

import java.io.File;
import java.io.IOException;
import java.util.List;

import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.SimpleParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import sun.rmi.runtime.Log;

public class Driver {
	public static String buffer = null;
	
	private static String getFiveDigitNumber(int num)
	{
		if(num<0 || num>100000)
		{
			System.out.println("Some error in getFiveDigitNumber!");
			return "ERROR";
		}
		if(num<10) return "0000"+num;
		if(num<100) return "000"+num;
		if(num<1000) return "00"+num;
		if(num<10000) return "0"+num;
		return ""+num;
	}

	public static void main(String args[]) throws Exception {
		System.out.println("Version 25-23-36-00");
		
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		//getJobFeatureVector(input, tmpdir + "/job_feature_vector");
		
		getHashtagFeatureVector(input, tmpdir + "/feature_vector");
		getHashtagSimilarities(tmpdir + "/feature_vector",
					output);
		
	}

	/**
	 * Computes the word cooccurrence counts for hashtag #job
	 * 
	 * @param input
	 *            The directory of input files. It can be local directory, such
	 *            as "data/", "/home/ubuntu/data/", or Amazon S3 directory, such
	 *            as "s3n://myawesomedata/"
	 * @param output
	 *            Same format as input
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getJobFeatureVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get feature vector for hashtag #Job");

		job.setClasses(JobMapper.class, JobReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.setReduceJobs(1);
	

		job.run();
	}

	/**
	 * Loads the computed word cooccurrence count for hashtag #job from disk.
	 * 
	 * @param dir
	 * @return
	 * @throws IOException
	 */
	private static List<String> loadFeatureVector(String dir) throws IOException {
		// Since there'll be only 1 reducer that process the key "#job", result
		// will be saved in the first result file, i.e., part-r-00000
		File f = new File(dir);
		if(!f.exists()) {
			System.out.println("********************************"+dir);
			return null;
		}
		List<String> featureVector = FileUtil.newLoad(dir);

		// The feature vector looks like "#job word1:count1;word2:count2;..."
		//String featureVector = job_featureVector.split("\\s+", 2)[1];
		return featureVector;
	}

	/**
	 * Same as getJobFeatureVector, but this one actually computes feature
	 * vector for all hashtags.
	 * 
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void getHashtagFeatureVector(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get feature vector for all hashtags");
		job.setClasses(HashtagMapper.class, HashtagReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.setReduceJobs(8);
		job.run();
	}

	/**
	 * When we have feature vector for both #job and all other hashtags, we can
	 * use them to compute inner products. The problem is how to share the
	 * feature vector for #job with all the mappers. Here we're using the
	 * "Configuration" as the sharing mechanism, since the configuration object
	 * is dispatched to all mappers at the beginning and used to setup the
	 * mappers.
	 * 
	 * @param jobFeatureVector
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getHashtagSimilarities(
			String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		// Share the feature vector of #job to all mappers.
		Configuration conf = new Configuration();
		//StringBuffer featureBuffer  = new StringBuffer();
		//for(int i = 0 ; i < jobFeatureVector.size(); i++)
		//	featureBuffer.append(jobFeatureVector.get(i) + "\t");
		//conf.set("FeatureVectors", featureBuffer.toString());
		//buffer = featureBuffer.toString();
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Get similarities between each part and all other hashtags");
		job.setClasses(SimilarityMapper.class, SimilarityReducer.class, null);
		job.setMapOutputClasses(Text.class,IntWritable.class);
		job.setReduceJobs(1);
		job.run();
	}
}
