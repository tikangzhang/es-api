package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketPipelineAggregationBuilder;

public class AggTopMinCondition extends AggTopLevelCondition{
	private MinBucketPipelineAggregationBuilder qb;

	public AggTopMinCondition(String colName,String path){
		this.qb = PipelineAggregatorBuilders.minBucket(colName, path);
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
