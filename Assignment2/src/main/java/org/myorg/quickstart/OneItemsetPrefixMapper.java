package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class OneItemsetPrefixMapper implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

	private static final long serialVersionUID = 1L;
	
	@Override
	public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> in) {
		return new Tuple2<Integer, Integer>(1, in.f0);
	}
}
