package com.laozhang.es.base.condition;

import org.elasticsearch.index.query.QueryBuilder;

public abstract class FunctionCondition {
	public abstract QueryBuilder getCondition();
}
