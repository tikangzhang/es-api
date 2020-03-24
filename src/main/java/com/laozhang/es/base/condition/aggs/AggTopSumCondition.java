package com.laozhang.es.base.condition.aggs;

import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregationBuilder;

public class AggTopSumCondition extends AggTopLevelCondition{
	private SumBucketPipelineAggregationBuilder qb;

	public AggTopSumCondition(String colName,String path){
		this.qb = PipelineAggregatorBuilders.sumBucket(colName, path);
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
