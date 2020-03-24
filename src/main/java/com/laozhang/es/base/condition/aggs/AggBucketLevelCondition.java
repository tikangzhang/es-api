package com.laozhang.es.base.condition.aggs;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

public abstract class AggBucketLevelCondition extends AggCondition{
	
	protected final static int DEFAULT_AGG_SIZE = Integer.MAX_VALUE;
	
	private AggTopSelectCondition tb;

	public AggBucketLevelCondition(){}
	
	public void addSubAgg(AggCondition a){
		this.cursor = addAgg(a);
	}
	
	public void addMetricsAgg(AggCondition a){
		addAgg(a);
	}
	
	public void addPipeLineAgg(AggCondition a){
		//TermsAggregationBuilder temp = (TermsAggregationBuilder)this.cursor;
		//temp.subAggregation((PipelineAggregationBuilder)a.getCondition());
		this.cursor.subAggregation((PipelineAggregationBuilder)a.getCondition());
	}
	
	public void all(){
		if(this.tb == null){
			this.tb = new AggTopSelectCondition(this.cursor.getName());
		}
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
	
	public void endTop(){
		TermsAggregationBuilder temp = (TermsAggregationBuilder)this.cursor;
		temp.subAggregation(tb.getCondition());
	}
	
	private AggregationBuilder addAgg(AggCondition a){
		aggBucketLevelReset();
		AggregationBuilder temp = (AggregationBuilder) a.getCondition();
		this.cursor.subAggregation(temp);
		return temp;
	}

	protected abstract void aggBucketLevelReset();
}
