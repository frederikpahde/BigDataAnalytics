package org.myorg.quickstart;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class CandidateCheckMapper implements FlatMapFunction<Tuple2<Integer[],Integer>, Tuple2<Integer[],Integer>>{

	
	/**
	 * Counts 1-ItemSets
	 */
	private static final long serialVersionUID = 1L;
	private ArrayList<Integer> basket;
	
	public CandidateCheckMapper(ArrayList<Integer> basket) {
		this.basket = basket;
	}

	@Override
	public void flatMap(Tuple2<Integer[], Integer> input, Collector<Tuple2<Integer[], Integer>> out) throws Exception {
		//if(basket.contains(input.f0)){
		//	out.collect(new Tuple2<Integer[], Integer>(input.f0, 1));
		//}
		Integer[] a = {0,1};
		out.collect(new Tuple2<Integer[], Integer>(a, 1));
		
	}

	
}
