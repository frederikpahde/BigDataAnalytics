package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public final class ConfidenceMapper implements FlatMapFunction<Tuple2<Tuple3<int[], Integer, Integer>, Tuple2<int[], Integer>>, Tuple3<int[], Integer, Double>> {

	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(Tuple2<Tuple3<int[], Integer, Integer>, Tuple2<int[], Integer>> in,
			Collector<Tuple3<int[], Integer, Double>> out) throws Exception {
		int totalSupport = in.f0.f2;
		int prefixSupport = in.f1.f1;
		out.collect(new Tuple3<int[], Integer, Double>(in.f0.f0, in.f0.f1, (1.0 * totalSupport)/prefixSupport));
	}
}
