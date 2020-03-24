package com.laozhang.es.base.condition.aggs;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;

public class AggHistogramCondition extends AggCondition{
	
	private HistogramAggregationBuilder qb;
	
	private List<BucketOrder> orderList;
	
	private AggTopSelectCondition tb;
	
	public AggHistogramCondition(String colName){
		this.qb = AggregationBuilders.histogram(colName).field(colName);
		this.cursor = this.qb;
	}
	
	public void setInterval(double inverval){
		this.qb.interval(inverval);
	}
	
	public void addMetricsAgg(AggCondition a){
		this.qb.subAggregation((AggregationBuilder) a.getCondition());
	}
	
	@Override
	public AggregationBuilder getCondition() {
		return this.qb;
	}
	
	public void top(String... cols){
		if(this.tb == null){
			this.tb = new AggTopSelectCondition(this.cursor.getName());
			this.tb.select(cols);
		}
	}
	
	public void top(String alias,String script){
		if(this.tb == null){
			this.tb = new AggTopSelectCondition(this.cursor.getName(),alias,script);
		}
	}
	
	public void topOrderBy(String orderCol,boolean desc){
		if(!StringUtils.isEmpty(orderCol)){
			if(this.tb != null){
				this.tb.order(orderCol,desc);
			}
		}
	}
	
	public void addScriptTop(String alias,String script){
		if(this.tb != null){
			this.tb.addScriptSelect(alias, script);
		}
	}
	
	public void size(Integer size){
		if(this.tb == null){
			this.tb.size(size);
		}
	}
	
	public void endTop(){
		DateHistogramAggregationBuilder temp = (DateHistogramAggregationBuilder)this.cursor;
		temp.subAggregation(tb.getCondition());
	}
	public void orderBy(String orderCol,boolean desc){
		if(null == orderList){
			orderList = new LinkedList<BucketOrder>();
		}
		orderList.add(BucketOrder.aggregation(orderCol, !desc));
		HistogramAggregationBuilder temp = (HistogramAggregationBuilder)this.qb;
		temp.order(BucketOrder.compound(orderList));
	}
	
	@Override
	public String getName(){
		return this.qb.getName();
	}
	
	public String getSubAggName(int index){
		return this.qb.getSubAggregations().get(index).getName();
	}
	
	public int getSubAggCount(){
		return this.qb.getSubAggregations().size();
	}
}
