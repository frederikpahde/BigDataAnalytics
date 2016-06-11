package org.myorg.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public final class OneItemsetPrefixMapper implements MapFunction<Tuple2<int[], Integer>, Tuple2<int[], Integer>> {

	private static final long serialVersionUID = 1L;
	
	/**
	 * Input: <int[] Two-Itemsets, int count>
	 */
	@Override
	public Tuple2<int[], Integer> map(Tuple2<int[], Integer> in) {
		int[] key = new int[in.f0.length-1];
		for (int i = 0; i < key.length; i++) {
			key[i] = in.f0[i];
		}
		return new Tuple2<int[], Integer>(key, in.f0[in.f0.length-1]);
	}
}
