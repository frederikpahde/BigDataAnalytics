package org.myorg.quickstart;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public final class ItemsetSplitMapper implements FlatMapFunction<Tuple2<int[], Integer>, Tuple3<int[], Integer, Integer>> {

	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Tuple2<int[], Integer> in, Collector<Tuple3<int[], Integer, Integer>> out) {
		for (int i = 0; i < in.f0.length; i++) {
			out.collect(new Tuple3<>(ArrayUtils.remove(in.f0, i), in.f0[i], in.f1));
		}
	}
}
