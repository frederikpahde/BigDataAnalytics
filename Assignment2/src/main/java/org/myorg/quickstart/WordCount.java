package org.myorg.quickstart;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

	

	public static <O> void main(String[] args) throws Exception {
		double minsupport = 0.01;
		double minconf = 0.1;
		
		String csvFile = "C:/Users/D059348/Documents/HU/SemII/BDA/BMS-POS.dat";
		
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Integer, Integer>> csvInput = env.readCsvFile(csvFile).fieldDelimiter("\t").types(Integer.class, Integer.class);

		//Count 1-Itemsets
		DataSet<Tuple2<Integer, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				csvInput.flatMap(new OneItemCounter())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);

		// execute and print result
		//counts.print();
		
		//Filter frequent 1-Itemsets
		DataSet<Tuple2<Integer, Integer>> frequentOneItemsets =
				counts.flatMap(new FrequentOneItemMapper((int)(minsupport * counts.count())));
		
		System.out.println(frequentOneItemsets.count());
		
		//Create 2-itemsets candidates
		DataSet<Tuple2<Integer, Integer>> prefixes =
				frequentOneItemsets.map(new OneItemsetPrefixMapper());
		
		
		DataSet<Tuple2<Integer[], Integer>> candidates = 
				prefixes.groupBy(0)
				.reduceGroup(new CandidateReducer());
		
		
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
		
		
		baskets.print();
	
		
		//System.out.println(candidates.count());
		//candidates.print();

		//System.out.println("#frequent1itemsets: " + frequentOneItemsets.count());

	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
