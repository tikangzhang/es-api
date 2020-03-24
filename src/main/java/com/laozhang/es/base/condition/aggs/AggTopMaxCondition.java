package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketPipelineAggregationBuilder;

public class AggTopMaxCondition extends AggTopLevelCondition{
	private MaxBucketPipelineAggregationBuilder qb;

	public AggTopMaxCondition(String colName,String path){
		this.qb = PipelineAggregatorBuilders.maxBucket(colName, path);
	}
	
	@Override
	public PipelineAggregationBuilder getCondition() {
		return this.qb;
	}

	@Override
	public String getName() {
		return this.qb.getName();
	}
}
