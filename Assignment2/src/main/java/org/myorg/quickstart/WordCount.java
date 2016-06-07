package org.myorg.quickstart;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

	

	public static void main(String[] args) throws Exception {
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
		
		frequentOneItemsets.print();

		//ystem.out.println("#frequent1itemsets: " + frequentOneItemsets.count());

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
