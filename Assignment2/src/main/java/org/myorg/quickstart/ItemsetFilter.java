package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public final class ItemsetFilter extends RichFilterFunction<Tuple2<int[],Integer>> implements FilterFunction<Tuple2<int[],Integer>> {
	
	private static final long serialVersionUID = 1L;
	private int minsupport;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		minsupport =  parameters.getInteger("minsupport", -1);
	}

	@Override
	public boolean filter(Tuple2<int[], Integer> input) throws Exception {
		if (input.f1 > minsupport){
			return true;
		}
		return false;
	}
}
