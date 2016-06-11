package org.myorg.quickstart;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class CandidateReducer implements GroupReduceFunction<Tuple2<int[],Integer>, Tuple2<int[], Integer>> {

	private static final long serialVersionUID = 1L;
	

	@Override
	public void reduce(Iterable<Tuple2<int[], Integer>> in, Collector<Tuple2<int[], Integer>> out) throws Exception {
		ArrayList<Integer> tmp = new ArrayList<>();
		int[] prefix = new int[0];
		
		for (Tuple2<int[], Integer> tupel1 : in) {
			prefix = tupel1.f0;
			tmp.add(tupel1.f1);
		}
		
		for (int f1 : tmp) {
			for (int f2 : tmp) {
				if (f1 < f2){
					int[] suffix = {f1,f2};
					int[] cand = new int[prefix.length + suffix.length];
					System.arraycopy(prefix, 0, cand, 0, prefix.length);
					System.arraycopy(suffix, 0, cand, prefix.length, suffix.length);
					out.collect(new Tuple2<int[], Integer>(cand, 0));
				}
			}
		}
	}
}
