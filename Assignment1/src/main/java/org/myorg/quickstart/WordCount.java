package org.myorg.quickstart;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;
/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class WordCount {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {
		String csvFile = "C:/Users/D059348/Downloads/T201212PDP+IEXT.CSV";
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple9<String, String, String, String, String, String, String, Double, String>> csvInput = env.readCsvFile(csvFile).ignoreFirstLine()
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, Double.class, String.class);


		DataSet<Tuple2<String, Double>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				csvInput.flatMap(new LineSplitter())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.sum(1);
		counts.print();

	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<Tuple9<String, String, String, String, String, String, String, Double, String>, Tuple2<String, Double>> {

		@Override
		public void flatMap(Tuple9<String, String, String, String, String, String, String, Double, String> csv, Collector<Tuple2<String, Double>> out) {
			out.collect(new Tuple2<String, Double>(csv.f2, csv.f7));
		}
	}
}
