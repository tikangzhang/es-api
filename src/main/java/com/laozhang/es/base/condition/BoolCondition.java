package com.laozhang.es.base.condition;

import java.util.Collection;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class BoolCondition extends FunctionCondition{
	BoolQueryBuilder qb;
	
	public BoolCondition(){
		this.qb = QueryBuilders.boolQuery();
	}
	
	public BoolCondition addSubAnd(BoolCondition c){
		qb.must(c.getCondition());
		return this;
	}
	
	public BoolCondition addSubOr(BoolCondition c){
		qb.should(c.getCondition());
		return this;
	}
	
	public BoolCondition addSubNot(BoolCondition c){
		qb.mustNot(c.getCondition());
		return this;
	}
	
	public <T> BoolCondition And(String k, T v){
		qb.must(QueryBuilders.termQuery(k, v));
		return this;
	}
	
	public BoolCondition And(String k, Object...vs){
		qb.must(QueryBuilders.termsQuery(k, vs));
		return this;
	}
	
	public BoolCondition And(String k, Collection<?> vs){
		qb.must(QueryBuilders.termsQuery(k, vs));
		return this;
	}
	public BoolCondition And(BoolCondition bc){
		qb.must(bc.getCondition());
		return this;
	}

	public BoolCondition AndPrefix(String k, String v){
		qb.must(QueryBuilders.prefixQuery(k,v));
		return this;
	}
	public BoolCondition AndWildcard(String k, String v){
		qb.must(QueryBuilders.wildcardQuery(k,v));
		return this;
	}
	public BoolCondition AndRegexp(String k, String v){
		qb.must(QueryBuilders.regexpQuery(k,v));
		return this;
	}
	public BoolCondition AndFuzzy(String k, String v){
		qb.must(QueryBuilders.fuzzyQuery(k,v));
		return this;
	}
	
	public <T> BoolCondition Or(String k, T v){
		qb.should(QueryBuilders.termQuery(k, v));
		return this;
	}
	
	public BoolCondition Or(String k, Object...vs){
		qb.should(QueryBuilders.termsQuery(k, vs));
		return this;
	}

	public BoolCondition Or(String k, Collection<?> vs){
		qb.should(QueryBuilders.termsQuery(k, vs));
		return this;
	}

	public BoolCondition Or(BoolCondition bc){
		qb.should(bc.getCondition());
		return this;
	}

	public BoolCondition OrPrefix(String k, String v){
		qb.should(QueryBuilders.prefixQuery(k, v));
		return this;
	}
	public BoolCondition OrWildcard(String k, String v){
		qb.should(QueryBuilders.wildcardQuery(k, v));
		return this;
	}
	public BoolCondition OrRegexp(String k, String v){
		qb.should(QueryBuilders.regexpQuery(k, v));
		return this;
	}
	public BoolCondition OrFuzzy(String k, String v){
		qb.should(QueryBuilders.fuzzyQuery(k, v));
		return this;
	}
	
	public <T> BoolCondition Not(String k, T v){
		qb.mustNot(QueryBuilders.termQuery(k, v));
		return this;
	}
	
	public BoolCondition Not(String k, Object...vs){
		qb.mustNot(QueryBuilders.termsQuery(k, vs));
		return this;
	}
	
	public BoolCondition Not(String k, Collection<?> vs){
		qb.mustNot(QueryBuilders.termsQuery(k, vs));
		return this;
	}

	public BoolCondition Not(BoolCondition bc){
		qb.mustNot(bc.getCondition());
		return this;
	}

	public BoolCondition NotPrefix(String k, String v){
		qb.mustNot(QueryBuilders.prefixQuery(k, v));
		return this;
	}
	public BoolCondition NotWildcard(String k, String v){
		qb.mustNot(QueryBuilders.wildcardQuery(k, v));
		return this;
	}
	public BoolCondition NotRegexp(String k, String v){
		qb.mustNot(QueryBuilders.regexpQuery(k, v));
		return this;
	}
	public BoolCondition NotFuzzy(String k, String v){
		qb.mustNot(QueryBuilders.fuzzyQuery(k, v));
		return this;
	}
	
	public BoolCondition AndRange(RangeCondition r){
		qb.filter(r.getCondition());
		return this;
	}
	
	public BoolCondition OrRange(RangeCondition r){
		qb.should(r.getCondition());
		return this;
	}
	
	public BoolCondition IsNotNull(String k){
		qb.must(QueryBuilders.existsQuery(k));
		return this;
	}
	
	public BoolCondition IsNull(String k){
		qb.mustNot(QueryBuilders.existsQuery(k));
		return this;
	}
	
	@Override
	public QueryBuilder getCondition() {
		return this.qb;
	}
}
