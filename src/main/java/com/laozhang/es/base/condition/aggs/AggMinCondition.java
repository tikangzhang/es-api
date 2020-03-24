package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;

import com.laozhang.es.base.condition.common.Arithmetic;

import java.util.Map;

public class AggMinCondition extends AggCondition{
	private MinAggregationBuilder qb;
	
	public final static String DEFAULT_ALIAS_PREFIX = "min_";
	
	public AggMinCondition(String colName){
		this.qb = AggregationBuilders.min(DEFAULT_ALIAS_PREFIX.concat(colName)).field(colName);
	}
	
	public AggMinCondition(String colName,String alias){
		this.qb = AggregationBuilders.min(alias).field(colName);
	}
	
	public AggMinCondition(String colName,String alias,String script){
		this.qb = AggregationBuilders.min(alias).script(getScript(script));
	}
	public AggMinCondition(String colName, String alias, String script, Map<String,Object> params){
		this.qb = AggregationBuilders.min(alias).script(getScript(script,params));
	}
	public AggMinCondition(String colName,String alias,Arithmetic c,Object o){
		this.qb = AggregationBuilders.min(alias).field(colName).script(getScript(c,o));
	}
	
	@Override
	public AggregationBuilder getCondition() {
		return this.qb;
	}

	@Override
	public String getName() {
		return this.qb.getName();
	}
}
