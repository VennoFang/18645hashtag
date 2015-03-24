package mapred.hashtagsim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	Map<String, Integer> jobFeatures = null;
	Log log = LogFactory.getLog(SimilarityMapper.class);
	Map<String, Integer> features_Vector = new HashMap<String, Integer>();
	List<Map<String,Integer>> allFeatures = new ArrayList<Map<String,Integer>>();
	List<String> hashtagNames = new ArrayList<String>();
	/**
	 * We compute the inner product of feature vector of every hashtag with that
	 * of #job
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//log.info("[KEY]"+key);
		//log.info("[VALUE]"+value);
		//log.info("[CONTEXT]"+context);
		String line = value.toString();
		
		//log.info("[LINE]"+line);
		
		//String featureTemp = context.getConfiguration().get("FeatureVectors");
		String featureTemp = Driver.buffer;
		//log.info("CALLED!!!");
		//starting value parsing
		String[] valueSplit = line.split("\t");
		List<String> hashtagNamesMapperInput = new ArrayList<String>();
		List<Map<String,Integer>> allFeaturesMapperInput = new ArrayList<Map<String,Integer>>();
		//rewrite here to suit the input format
		for(int i=0; i<valueSplit.length - valueSplit.length%2; i+=2) {
			String hashtag = valueSplit[i];
			hashtagNamesMapperInput.add(hashtag);
			Map<String, Integer> features_Vectors = parseFeatureVector(valueSplit[i+1]);
			allFeaturesMapperInput.add(features_Vectors);
		}
		
		
		
		//start driver passing
		
		
		//String[] hashtag_featureVector = line.split("\\s+", 2);

		//String hashtag = hashtag_featureVector[0];
		//Map<String, Integer> features = parseFeatureVector(hashtag_featureVector[1]);

		
		ArrayList<ArrayList<Integer>> similarity = multipleInnerProductAtATime
				(
						allFeaturesMapperInput, 
						allFeatures,
						hashtagNamesMapperInput,
						hashtagNames);
		for(int i=0; i<similarity.size(); i++) {
			for(int j=0; j<similarity.get(i).size(); j++) {
				//if(i==j) continue;
				String hashtag1 = hashtagNamesMapperInput.get(i);
				String hashtag2 = hashtagNames.get(j);
				//log.info("[1]"+hashtag1);
				//log.info("[2]"+hashtag2);
				if(hashtag1.compareTo(hashtag2) >= 0) continue;
				
				context.write(new IntWritable(similarity.get(i).get(j)), new Text(hashtag1 + " " + hashtag2));
			}
		}
		//context.write(new IntWritable(similarity), new Text("#job\t" + hashtag));
	}

	/**
	 * This function is ran before the mapper actually starts processing the
	 * records, so we can use it to setup the job feature vector.
	 * 
	 * Loads the feature vector for hashtag #job into mapper's memory
	 */
	@Override
	protected void setup(Context context) {
		//String jobFeatureVector = context.getConfiguration().get(
		//		"jobFeatureVector");
		//jobFeatures = parseFeatureVector(jobFeatureVector);
		String featureTemp = Driver.buffer;
		String[] features = featureTemp.split("\t");
		
		
		//rewrite here to suit the input format
		for(int i=0; i<features.length - features.length%2; i+=2) {
			//String[] feature = features[i+1].split("\\s+", 2);
			//log.info("**Feature= "+features[i]);
			String hashtag = features[i];
			hashtagNames.add(hashtag);
			features_Vector = parseFeatureVector(features[i+1]);
			allFeatures.add(features_Vector);
		}
	}

	/**
	 * De-serialize the feature vector into a map
	 * 
	 * @param featureVector
	 *            The format is "word1:count1;word2:count2;...;wordN:countN;"
	 * @return A HashMap, with key being each word and value being the count.
	 */
	private Map<String, Integer> parseFeatureVector(String featureVector) {
		Map<String, Integer> featureMap = new HashMap<String, Integer>();
		String[] features = featureVector.split(";");
		for (String feature : features) {
			String[] word_count = feature.split(":");
			featureMap.put(word_count[0], Integer.parseInt(word_count[1]));
		}
		return featureMap;
	}

	/**
	 * Computes the dot product of two feature vectors
	 * @param featureVector1
	 * @param featureVector2
	 * @return 
	 */
	private Integer computeInnerProduct(Map<String, Integer> featureVector1,
			Map<String, Integer> featureVector2) {
		Integer sum = 0;
		for (String word : featureVector1.keySet()) 
			if (featureVector2.containsKey(word))
				sum += featureVector1.get(word) * featureVector2.get(word);
		
		return sum;
	}
	
	private ArrayList<ArrayList<Integer>> multipleInnerProductAtATime(
			List<Map<String,Integer>> featureVectors1,
			List<Map<String,Integer>> featureVectors2,
			List<String> fName1,
			List<String> fName2
			)
	{
		ArrayList<ArrayList<Integer>> result = new ArrayList<ArrayList<Integer>>();
		int count1 = featureVectors1.size();
		int count2 = featureVectors2.size();
		for(int i = 0; i < count1; i++)
		{
			ArrayList<Integer> newArray = new ArrayList<Integer>();
			result.add(new ArrayList<Integer>());
		}
		for(int i = 0; i < count1; i++)
		{
			for(int j = 0; j < count2; j++)
			{
				
				if(fName1.get(i).compareTo(fName2.get(j)) >= 0)
				{
					result.get(i).add(0);
				}
				else
				result.get(i).add(computeInnerProduct(featureVectors1.get(i),featureVectors2.get(j)));
			}
		}
		return result;
		
	}
	
}














