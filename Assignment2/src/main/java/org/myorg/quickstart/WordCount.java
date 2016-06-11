package org.myorg.quickstart;


import java.util.ArrayList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class WordCount {

	public static <O> void main(String[] args) throws Exception {
		double minsupport = 0.01;
		double minconf = 0.5;
		Configuration config = new Configuration();
		
		String csvFile = "C:/Users/D059348/Documents/HU/SemII/BDA/BMS-POS.dat";
		
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Integer, Integer>> csvInput = env.readCsvFile(csvFile).fieldDelimiter("\t").types(Integer.class, Integer.class);
		
		//Creation of baskets <basketID, Arraylist with itemIDs>
		DataSet<Tuple2<Integer, ArrayList<Integer>>> baskets =
				csvInput.groupBy(0)
				.reduceGroup(new GroupReduceFunction<Tuple2<Integer,Integer>, Tuple2<Integer, ArrayList<Integer>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void reduce(Iterable<Tuple2<Integer, Integer>> iterator,
							Collector<Tuple2<Integer, ArrayList<Integer>>> out) throws Exception {
						ArrayList<Integer> list = new ArrayList<>();
						int basketID = -1;
						for (Tuple2<Integer, Integer> i : iterator) {
							if (basketID == -1){
								basketID = i.f0;
							}
							list.add(i.f1);
							
						}
						out.collect(new Tuple2<Integer, ArrayList<Integer>>(basketID, list));	
					}
				});
		
		config.setInteger("minsupport", (int)(minsupport * baskets.count()));
		config.setDouble("minconf", minconf);

		//Count 1-Itemsets
		DataSet<Tuple2<int[], Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				csvInput.flatMap(new OneItemCounter())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);
		
		//Filter frequent 1-Itemsets
		DataSet<Tuple2<int[], Integer>> frequentOneItemsets = counts.filter(new ItemsetFilter()).withParameters(config);
		System.out.println(frequentOneItemsets.count());
		
		ArrayList<DataSet<Tuple2<int[], Integer>>> result = new ArrayList<>();
		ArrayList<DataSet<Tuple3<int[], Integer, Double>>> resultRules = new ArrayList<>();
		result.add(frequentOneItemsets);
		
		int k = 1;
		while(true){
			DataSet<Tuple2<int[], Integer>> previousFrequentTuple = result.get(result.size()-1);
			DataSet<Tuple2<int[], Integer>> candidates = 
					previousFrequentTuple.map(new OneItemsetPrefixMapper())
					.groupBy(0)
					.reduceGroup(new CandidateReducer());
			
			DataSet<Tuple2<int[], Integer>> frequentItemsets = 
					baskets.flatMap(new ItemsetCounter())
					.withBroadcastSet(candidates, "candidates")		
					.groupBy(0)
					.sum(1)
					.filter(new ItemsetFilter()).withParameters(config);
			
			if (frequentItemsets.count() == 0){
				break;
			}else{
				result.add(frequentItemsets);
			}
			
			//Find Association Rules
			DataSet<Tuple3<int[], Integer, Double>> rules = frequentItemsets.flatMap(new ItemsetSplitMapper())
			.join(result.get(result.size()-2))
			.where(0).equalTo(0)
			.flatMap(new ConfidenceMapper())
			.filter(new ConfidenceFilter())
			.withParameters(config);
			
			rules.writeAsFormattedText("C:/Users/D059348/Documents/result"+k+".csv", new TextFormatter<Tuple3<int[],Integer,Double>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String format(Tuple3<int[], Integer, Double> in) {
					String res = "[";
					for (int i : in.f0) {
						res = res + i + ",";
					}
					res = res.substring(0, res.length()-1) + "]";
					res = res + "\t --> \t" + in.f1 + " (" + in.f2 + " %)";
					return res;
				}
			});
			//rules.writeAsCsv("C:/Users/D059348/Documents/result"+k+".csv");
			resultRules.add(rules);
			k++;
		}
		int i = 0;
		int[] ruleCounts = new int[resultRules.size()];
		for (DataSet<Tuple3<int[], Integer, Double>> dataSet : resultRules) {
			//dataSet.writeAsCsv("C:/Users/D059348/Documents/result.csv");
			ruleCounts[i] = (int) dataSet.count();
			//i++;
			//System.out.println("write reslt");
		
		}
		System.out.println(resultRules.size());
		
		for (int j = 0; j < ruleCounts.length; j++) {
			System.out.println("number rules: " + ruleCounts[j]);
		}
	}
}
