package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;

import com.laozhang.es.base.condition.common.Arithmetic;

import java.util.Map;

public class AggMaxCondition extends AggCondition{
	private MaxAggregationBuilder qb;
	
	public final static String DEFAULT_ALIAS_PREFIX = "max_";
	
	public AggMaxCondition(String colName){
		this.qb = AggregationBuilders.max(DEFAULT_ALIAS_PREFIX.concat(colName)).field(colName);
	}
	
	public AggMaxCondition(String colName,String alias){
		this.qb = AggregationBuilders.max(alias).field(colName);
	}
	
	public AggMaxCondition(String colName,String alias,String script){
		this.qb = AggregationBuilders.max(alias).script(getScript(script));
	}
	public AggMaxCondition(String colName, String alias, String script, Map<String,Object> params){
		this.qb = AggregationBuilders.max(alias).script(getScript(script,params));
	}
	public AggMaxCondition(String colName,String alias,Arithmetic c,Object o){
		this.qb = AggregationBuilders.max(alias).field(colName).script(getScript(c,o));
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
