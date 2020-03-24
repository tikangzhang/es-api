package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;

import com.laozhang.es.base.condition.common.Arithmetic;

import java.util.Map;

public class AggAvgCondition extends AggCondition{
	private AvgAggregationBuilder qb;
	
	public final static String DEFAULT_ALIAS_PREFIX = "avg_";
	
	public AggAvgCondition(String colName){
		this.qb = AggregationBuilders.avg(DEFAULT_ALIAS_PREFIX.concat(colName)).field(colName);
	}
	
	public AggAvgCondition(String colName,String alias){
		this.qb = AggregationBuilders.avg(alias).field(colName);
	}
	
	public AggAvgCondition(String colName,String alias,String script){
		this.qb = AggregationBuilders.avg(alias).script(getScript(script));
	}

	public AggAvgCondition(String colName, String alias, String script, Map<String,Object> params){
		this.qb = AggregationBuilders.avg(alias).script(getScript(script,params));
	}
	
	public AggAvgCondition(String colName,String alias,Arithmetic c,Object o){
		this.qb = AggregationBuilders.avg(alias).field(colName).script(getScript(c,o));
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
