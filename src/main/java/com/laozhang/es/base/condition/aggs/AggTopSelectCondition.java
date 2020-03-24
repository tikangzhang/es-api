package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class AggTopSelectCondition extends AggCondition{
	private TopHitsAggregationBuilder qb;
	
	public final static String DEFAULT_TOP_NAME = "TOP_ONE";
	
	public final static int TOP_SIZE = 1; //只取第一条
	
	public AggTopSelectCondition(String colName){
		this.qb = AggregationBuilders.topHits(colName).size(TOP_SIZE);
	}
	
	public AggTopSelectCondition(String colName, String alias, String script){
		this.qb = AggregationBuilders.topHits(colName).size(TOP_SIZE).scriptField(alias, getScript(script));
	}
	
	public AggTopSelectCondition size(int size){
		this.qb.size(size);
		return this;
	}
	
	public AggTopSelectCondition select(String[] cols){
		this.qb.fetchSource(cols, null);
		return this;
	}
	
	public AggTopSelectCondition addScriptSelect(String alias, String script){
		this.qb.scriptField(alias, getScript(script));
		return this;
	}
	
	public AggTopSelectCondition order(String orderCol,boolean desc){
		if(desc){
			this.qb.sort(orderCol, SortOrder.DESC);
		}else{
			this.qb.sort(orderCol, SortOrder.ASC);
		}
		return this;
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
