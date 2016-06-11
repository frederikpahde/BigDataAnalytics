package org.myorg.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public final class ArrayToStringMapper implements MapFunction<Tuple3<int[], Integer, Double>, String> {

	private static final long serialVersionUID = 1L;
	
	/**
	 * Input: <int[] Two-Itemsets, int count>
	 */
	@Override
	public String map(Tuple3<int[], Integer, Double> in) {
		String res = "[";
		for (int i : in.f0) {
			res = res + i + ",";
		}
		res = res.substring(0, res.length()-1) + "]";
		res = res + "\t --> \t" + in.f1 + " (" + in.f2 + " %)";
		return res;
	}
}
