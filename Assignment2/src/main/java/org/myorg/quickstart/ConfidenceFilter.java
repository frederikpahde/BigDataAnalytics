package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

public final class ConfidenceFilter extends RichFilterFunction<Tuple3<int[],Integer, Double>> implements FilterFunction<Tuple3<int[],Integer,Double>> {
	
	private static final long serialVersionUID = 1L;
	private double minconf;
	
	@Override
	public void open(Configuration parameters) throws Exception {
		minconf =  parameters.getDouble("minconf", -1);
	}

	@Override
	public boolean filter(Tuple3<int[], Integer, Double> input) throws Exception {
		if (input.f2 > minconf){
			return true;
		}
		return false;
	}
}
