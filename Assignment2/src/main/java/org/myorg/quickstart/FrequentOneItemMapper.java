package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class FrequentOneItemMapper implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

	private static final long serialVersionUID = 1L;
	private int minsupport; //as total number
	
	public FrequentOneItemMapper(int minsupport) {
		this.minsupport = minsupport;
	}

	@Override
	public void flatMap(Tuple2<Integer, Integer> in, Collector<Tuple2<Integer, Integer>> out) {
		if (in.f1 >= minsupport){
			out.collect(new Tuple2<Integer, Integer>(in.f0, in.f1));
		}
	}
}
