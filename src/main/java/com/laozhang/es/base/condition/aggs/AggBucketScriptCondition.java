package com.laozhang.es.base.condition.aggs;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregationBuilder;

public class AggBucketScriptCondition extends AggCondition{
	private BucketScriptPipelineAggregationBuilder qb;
	
	public final static String DEFAULT_ALIAS_PREFIX = "script_";
	
	public AggBucketScriptCondition(String colAlias,String script,String...aggCols){
		Map<String,String> pathsMap = new HashMap<>();
		String key;
		for(int i = 0, len = aggCols.length; i < len; i++){
			key = DEFAULT_ALIAS_PREFIX + aggCols[i];
			pathsMap.put(key, aggCols[i]);
			script = script.replace(aggCols[i], "params." + key);
		}
		
		this.qb = PipelineAggregatorBuilders.bucketScript(colAlias, pathsMap, getScript(script));
		//this.qb = PipelineAggregatorBuilders.bucketScript(colAlias, getScript(script), aggCols);
	}
	
	public AggBucketScriptCondition(String colAlias,String script,Map<String,String> aggCols){
		this.qb = PipelineAggregatorBuilders.bucketScript(colAlias, aggCols, getScript(script));
	}
	
	public PipelineAggregationBuilder getCondition() {
		return this.qb;
	}

	public String getName() {
		return this.qb.getName();
	}
}
