package com.laozhang.es.base.condition.aggs;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder;

import com.laozhang.es.base.condition.common.LogicRelation;
import com.laozhang.es.base.condition.script.SimpleScriptFormater;

public class AggBucketSelectCondition<T> extends AggCondition{

	private BucketSelectorPipelineAggregationBuilder aspab;

	public final static String DEFAULT_ALIAS_PREFIX = "having_";
	
	public final static String DEFAULT_ALIAS = "default_having";
	
	public AggBucketSelectCondition(String aggCol, LogicRelation relation, T value){
		this(DEFAULT_ALIAS,aggCol,relation,value);
	}
	
	public AggBucketSelectCondition(String alias,String aggCol, LogicRelation relation, T value){
		Map<String,String> paths = new HashMap<String,String>();
		StringBuilder sb = new StringBuilder();
		paths.put(DEFAULT_ALIAS_PREFIX + aggCol, aggCol);
		sb.append(SimpleScriptFormater.PARAMS_PREFIX)
		.append(DEFAULT_ALIAS_PREFIX + aggCol)
		.append(relation.getName()).append(value);
		
		this.aspab = PipelineAggregatorBuilders.bucketSelector(alias, paths, getScript(sb.toString()));
	}
	
	@Override
	public PipelineAggregationBuilder getCondition() {
		return this.aspab;
	}
	
	@Override
	public String getName(){
		return this.aspab.getName();
	}
}
