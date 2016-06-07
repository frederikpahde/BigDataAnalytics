package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class OneItemCounter implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

	
	/**
	 * Counts 1-ItemSets
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Tuple2<Integer, Integer> in, Collector<Tuple2<Integer, Integer>> out) {
		out.collect(new Tuple2<Integer, Integer>(in.f1, 1));	
	}
}
