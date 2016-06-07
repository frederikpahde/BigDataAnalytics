package org.myorg.quickstart;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class CandidateReducer implements GroupReduceFunction<Tuple2<Integer,Integer>, Tuple2<Integer[], Integer>> {

	private static final long serialVersionUID = 1L;
	

	@Override
	public void reduce(Iterable<Tuple2<Integer, Integer>> in, Collector<Tuple2<Integer[], Integer>> out) throws Exception {
		ArrayList<Integer> tmp = new ArrayList<>();
		for (Tuple2<Integer, Integer> tupel1 : in) {
			tmp.add(tupel1.f1);
		}
		
		for (int f1 : tmp) {
			for (int f2 : tmp) {
				if (f1 < f2){
					Integer[] cand = {f1,f2};
					out.collect(new Tuple2<Integer[], Integer>(cand, 0));
				}
			}
		}
	}
}
