package org.myorg.quickstart;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public final class ItemsetCounter extends RichMapFunction implements FlatMapFunction<Tuple2<Integer, ArrayList<Integer>>, Tuple2<int[], Integer>> {
	
	private static final long serialVersionUID = 1L;
	private Collection<Tuple2<int[], Integer>> candidates;
	
	@Override
	public void flatMap(Tuple2<Integer, ArrayList<Integer>> input, Collector<Tuple2<int[], Integer>> out)
			throws Exception {
				//DataSet<Tuple2<Integer[], Integer>> resultset = candidates.flatMap(new CandidateCheckMapper(input.f1));
				//List<Tuple2<Integer[], Integer>> resultList = resultset.collect();
				//for (Tuple2<Integer[], Integer> tuple : resultList) {
				//	out.collect(tuple);
				//}
		//int i = 0;
		ArrayList<Integer> basket = input.f1;
		for (Tuple2<int[], Integer> candidate : candidates) {
			//if (i>100)break;
			//i++;
			
			boolean candidateIsInBasket = true;
			for (Integer item : candidate.f0) {
				candidateIsInBasket = candidateIsInBasket && basket.contains(item);
			}
			if (candidateIsInBasket){
				out.collect(new Tuple2<int[], Integer>(candidate.f0, 1));
			}
			
			
		}
		
		
		
	}

	@Override
	public Object map(Object arg0) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		this.candidates =  getRuntimeContext().getBroadcastVariable("candidates");
		
	}
}
