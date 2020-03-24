package com.laozhang.es.base.condition.aggs;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.joda.time.DateTimeZone;

import com.laozhang.es.base.CommonConstant;

public class AggTimeRangeStatCondition extends AggCondition{
	
	private DateRangeAggregationBuilder qb;
	
	public AggTimeRangeStatCondition(String colName){
		this.qb = AggregationBuilders.dateRange(colName).field(colName);
		this.qb.timeZone(DateTimeZone.forID(CommonConstant.COMMON_EAST8_TIME_ZONE));
	}
	
	public void setFormater(String formater){
		this.qb.format(formater);
	}
	
	public void setOffsetTimeZone(int offsetHours){
		this.qb.timeZone(DateTimeZone.forOffsetHours(offsetHours));
	}
	
	public void setTimeZone(String timeZone){
		this.qb.timeZone(DateTimeZone.forID(timeZone));
	}
	
	public void addMetricsAgg(AggCondition a){
		this.qb.subAggregation((AggregationBuilder) a.getCondition());
	}
	
	public void addPipeLineAgg(AggCondition a){
		DateRangeAggregationBuilder temp = (DateRangeAggregationBuilder)this.cursor;
		temp.subAggregation((PipelineAggregationBuilder)a.getCondition());
	}
	
	public void addRang(String start, String end, String key){
		this.qb.addRange(key, start, end);
	}
	
	public void addStart(String start, String key){
		this.qb.addUnboundedFrom(key, start);
	}
	
	public void addEnd(String start, String key){
		this.qb.addUnboundedTo(key, start);
	}
	
	@Override
	public AggregationBuilder getCondition() {
		return this.qb;
	}
	
	public void top(int size,String orderCol,boolean desc,String... cols){
		DateRangeAggregationBuilder temp = (DateRangeAggregationBuilder)this.qb;
		AggTopSelectCondition tb = new AggTopSelectCondition(temp.getName());
		
		tb.size(size);
		if(!StringUtils.isEmpty(orderCol)){
			tb.order(orderCol,desc);
		}
		tb.select(cols);
		
		temp.subAggregation(tb.getCondition());
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
