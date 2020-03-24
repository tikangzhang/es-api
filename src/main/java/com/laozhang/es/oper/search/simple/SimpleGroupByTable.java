package com.laozhang.es.oper.search.simple;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.laozhang.es.base.Executor;
import com.laozhang.es.base.condition.BodyCondition;
import com.laozhang.es.base.condition.BoolCondition;
import com.laozhang.es.base.condition.RangeCondition;
import com.laozhang.es.base.condition.aggs.*;
import com.laozhang.es.base.condition.common.Arithmetic;
import com.laozhang.es.base.condition.common.LogicRelation;
import com.laozhang.es.base.exception.DataBaseNameNullException;
import com.laozhang.es.oper.search.*;
import com.laozhang.es.base.utils.BeanCopierUtil;
import com.laozhang.es.base.utils.page.PageHelper;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleGroupByTable extends Executor implements ISimpleGroupByTable, IWhere, IGroupBy, IResult, IPipGroupBy {
	
	public static Logger logger = LoggerFactory.getLogger(SimpleGroupByTable.class);
	
	private String dataBase;
	
	private String table;
	
	private BodyCondition bodyCondition;
	
	private BoolCondition boolCondition;
	
	private RangeCondition rangCondition;
	
	private AggCondition aggCondition;
	
	private LinkedList<AggCondition> rootAggConditionList = new LinkedList<>();
	
	private Map<String,String> selAliasMap;
	
	public SimpleGroupByTable(String dataBaseName){
		this(dataBaseName,null);
	}
	
	public SimpleGroupByTable(String dataBaseName, String tableName){
		this.dataBase = dataBaseName;
		this.table = tableName;
	}
	
	public SimpleGroupByTable SelectAggCols() {
		if(null != this.aggCondition) {
			AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
			temp.all();
		}else{
			AggTopSelectCondition topCondition = new AggTopSelectCondition(AggTopSelectCondition.DEFAULT_TOP_NAME);
			this.rootAggConditionList.add(topCondition);
			logger.debug(String.format("add a top hit operation", "top"));
		}
		return this;
	}
	
	@Override
	public SimpleGroupByTable SelectAggCols(String...cols) {
		if(null != this.aggCondition) {
			logger.debug("select:" + cols);
			AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
			temp.top(cols);
		}else{
			AggTopSelectCondition topCondition = new AggTopSelectCondition("rootTop");
			topCondition.select(cols);
			this.rootAggConditionList.add(topCondition);
			logger.debug(String.format("add a root %s operation : cols %s ", "top", cols.toString()));
		}
		return this;
	}
	
	@Override
	public SimpleGroupByTable SelectAggScriptCols(String alias, String Script) {
		if(null != this.aggCondition) {
			logger.debug("select custom script cols:" + alias);
			AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
			temp.top(alias,Script);
		}else{
			AggTopSelectCondition topCondition = new AggTopSelectCondition("rootTop",alias,Script);
			this.rootAggConditionList.add(topCondition);
			logger.debug(String.format("add a root %s operation : cols %s ", "top script", Script));
		}
		
		return this;
	}
	
	@Override
	public SimpleGroupByTable OrderAggCols(String orderCol, boolean desc) {
		if(null != this.aggCondition) {
			logger.debug("select ordered custom cols:" + orderCol);
			AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
			temp.topOrderBy(orderCol, desc);
		}
		return this;
	}
	
	@Override
	public SimpleGroupByTable AppendAggScriptCol(String alias, String script) {
		if(null != this.aggCondition) {
			logger.debug("select custom Script cols:" + alias);
			AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
			temp.addScriptTop(alias, script);
		}else{
			for(AggCondition condition: this.rootAggConditionList){
				if(condition instanceof AggTopSelectCondition){
					AggTopSelectCondition topCondition = (AggTopSelectCondition)condition;
					topCondition.addScriptSelect(alias, script);
				}
			}
		}
		return this;
	}
	
	@Override
	public SimpleGroupByTable EndSelectAggCols(){
		if(null != this.aggCondition) {
			logger.debug("finish custom cols");
			AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
			temp.endTop();
		}
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereAnd(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable Where(BoolCondition bc) {
		boolCondition = bc;
		logger.debug("add a sub condition");
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereOr(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","or", k,String.valueOf(v)));
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereNot(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","not", k,String.valueOf(v)));
		return this;
	}
	
	@Override
	public <T> SimpleGroupByTable WhereAnd(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubAnd(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereOr(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubOr(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereNot(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubNot(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereRange(String k, T from, T to) {
		rangCondition = new RangeCondition(k);
		rangCondition.Gte(from).Lt(to);
		
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRange(rangCondition);
		logger.debug(String.format("add %s condition: %s -> %s","range", String.valueOf(from),String.valueOf(to)));
		return this;
	}
	
	@Override
	public <T> SimpleGroupByTable WhereRange(String k, LogicRelation relation, T to) {
		if(LogicRelation.Ne.equals(relation)){
			logger.error("do not support Ne relation");
			return this;
		}
		rangCondition = new RangeCondition(k);
		if(LogicRelation.Gt.equals(relation)){
			rangCondition.Gt(to);
		}else if(LogicRelation.Gte.equals(relation)){
			rangCondition.Gte(to);
		}else if(LogicRelation.Lt.equals(relation)){
			rangCondition.Lt(to);
		}else if(LogicRelation.Lte.equals(relation)){
			rangCondition.Lte(to);
		}else if(LogicRelation.Eq.equals(relation)){
			WhereAnd(k, to);
		}
		
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRange(rangCondition);
		logger.debug(String.format("add %s condition: %s %s","range", relation.getName(),to.toString()));
		return this;
	}
	
	@Override
	public <T> SimpleGroupByTable WhereRange(String k, T from, T to, String timeZone) {
		rangCondition = new RangeCondition(k);
		rangCondition.Gte(from).Lt(to);
		rangCondition.setTimeZone(timeZone);
		
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRange(rangCondition);
		logger.debug(String.format("add %s condition: %s -> %s","range", String.valueOf(from),String.valueOf(to)));
		return this;
	}
	
	@Override
	public IWhere WhereIsNotNull(String k) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.IsNotNull(k);;
		logger.debug(String.format("add a condition: %s is not null",k));
		return this;
	}

	@Override
	public IWhere WhereIsNull(String k) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.IsNull(k);;
		logger.debug(String.format("add a condition: %s is not null",k));
		return this;
	}
	
	@Override
	public SimpleGroupByTable GroupByWithAlias(String col, String alias) {
		if(StringUtils.isEmpty(col) || StringUtils.isEmpty(alias)){
			return this;
		}
		if(null != this.aggCondition){
			AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
			temp.addSubAgg(new AggTermsCondition(col,alias));
			logger.debug("add a sub agg:" + alias);
		}else{
			this.aggCondition = new AggTermsCondition(col,alias);
			logger.debug("add a agg:" + alias);
		}
		return this;
	}
	@Override
	public SimpleGroupByTable GroupBy(String... cols) {
		if(null == cols){
			return this;
		}
		if(cols.length == 1){
			String groupName = cols[0];
			if(null != this.aggCondition){
				AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
				temp.addSubAgg(new AggTermsCondition(groupName));
				logger.debug("add a sub agg:" + cols);
			}else{
				this.aggCondition = new AggTermsCondition(groupName);
				logger.debug("add a agg:" + cols);
			}
		}else{
			for(int i =0,len = cols.length; i < len ; i++){
				String groupName = cols[i];
				if(!StringUtils.isEmpty(groupName)){
					if(0 == i){
						this.aggCondition = new AggTermsCondition(groupName);
					}else{
						AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
						temp.addSubAgg(new AggTermsCondition(groupName));
					}
				}
			}
			logger.debug("add a group aggs:" + cols);
		}
		return this;
	}
	
	@Override
	public SimpleGroupByTable OrderBy(String alias, boolean desc){
		if(this.aggCondition instanceof AggTermsCondition){
			if(null != this.aggCondition) {
				AggTermsCondition temp = (AggTermsCondition)this.aggCondition;
				temp.orderBy(alias, desc);
				logger.debug("add a orderby conditions:" + alias);
			}
		}else{
			logger.warn("only support AggTermsCondition");
		}
		return this;
	}
	
	@Override
	public SimpleGroupByTable OrderByGroupName(boolean desc) {
		if(this.aggCondition instanceof AggTermsCondition){
			if(null != this.aggCondition) {
				AggTermsCondition temp = (AggTermsCondition)this.aggCondition;
				temp.orderByTerms(desc);
				logger.debug("add a order by group conditions:" + desc);
			}
		}else{
			logger.warn("only support AggTermsCondition");
		}
		return this;
	}

	@Override
	public SimpleGroupByTable OrderByGroupRecordCount(boolean desc) {
		if(this.aggCondition instanceof AggTermsCondition){
			if(null != this.aggCondition) {
				AggTermsCondition temp = (AggTermsCondition)this.aggCondition;
				temp.orderByRecordCount(desc);
				logger.debug("add a order by group count conditions:" + desc);
			}
		}else{
			logger.warn("only support AggTermsCondition");
		}
		return this;
	}
	
	@Override
	public SimpleGroupByTable AggSize(Integer size) {
		if(this.aggCondition instanceof AggTermsCondition){
			AggTermsCondition temp = (AggTermsCondition)this.aggCondition;
			if(null != temp){
				temp.size(size);
				logger.debug("set limit:" + size);
			}
		}else{
			logger.warn("only support AggTermsCondition");
		}
		return this;
	}

	@Override
	public SimpleGroupByTable Sum(String col) {
		return Sum(col,AggSumCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public SimpleGroupByTable Sum(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "sum",col,alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "sum",col,alias));
		return this;
	}
	
	@Override
	public SimpleGroupByTable Sum(String col, Arithmetic opType, Object opNum) {
		return Sum(col,AggSumCondition.DEFAULT_ALIAS_PREFIX.concat(col),opType,opNum);
	}
	
	@Override
	public SimpleGroupByTable Sum(String col, String alias, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s with alias %s", "sum",col,opType.getName(),String.valueOf(opNum),alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,alias,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s with alias %s", "sum",col,opType.getName(),String.valueOf(opNum),alias));
		return this;
	}
	
	@Override
	public SimpleGroupByTable Avg(String col) {
		return Avg(col,AggAvgCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}

	@Override
	public SimpleGroupByTable Avg(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "avg",col,alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "avg",col,alias));
		return this;
	}
	
	@Override
	public SimpleGroupByTable Avg(String col, Arithmetic opType, Object opNum) {
		return Avg(col,AggAvgCondition.DEFAULT_ALIAS_PREFIX.concat(col),opType,opNum);
	}
	
	@Override
	public SimpleGroupByTable Avg(String col, String alias, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s with alias %s", "avg",col,opType.getName(),String.valueOf(opNum),alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,alias,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s with alias %s", "avg",col,opType.getName(),String.valueOf(opNum),alias));
		return this;
	}

	@Override
	public SimpleGroupByTable Max(String col) {
		return Max(col,AggMaxCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public SimpleGroupByTable Max(String col, Arithmetic opType, Object opNum) {
		return Max(col,AggMaxCondition.DEFAULT_ALIAS_PREFIX.concat(col),opType,opNum);
	}
	
	@Override
	public SimpleGroupByTable Max(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "max",col,alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "max",col,alias));
		return this;
	}
	
	@Override
	public SimpleGroupByTable Max(String col, String alias, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s with alias %s", "max",col,opType.getName(),String.valueOf(opNum),alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,alias,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s with alias %s", "max",col,opType.getName(),String.valueOf(opNum),alias));
		return this;
	}

	@Override
	public SimpleGroupByTable Min(String col) {
		return Min(col,AggMinCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public SimpleGroupByTable Min(String col, Arithmetic opType, Object opNum) {
		return Min(col,AggMinCondition.DEFAULT_ALIAS_PREFIX.concat(col),opType,opNum);
	}
	
	@Override
	public SimpleGroupByTable Min(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "min",col,alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "min",col,alias));
		return this;
	}
	
	@Override
	public SimpleGroupByTable Min(String col, String alias, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s with alias %s", "min",col,opType.getName(),String.valueOf(opNum),alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,alias,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s with alias %s", "min",col,opType.getName(),String.valueOf(opNum),alias));
		return this;
	}

	@Override
	public SimpleGroupByTable Count(String col) {
		return Count(col,AggCountCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public SimpleGroupByTable Count(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggCountCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "count",col,alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggCountCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "count",col,alias));
		return this;
	}
	
	@Override
	public SimpleGroupByTable DistinctCount(String col) {
		return DistinctCount(col,AggDistinctCountCondition.DEFAULT_ALIAS_PREFIX.concat(col));
	}
	
	@Override
	public SimpleGroupByTable DistinctCount(String col, String alias) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggDistinctCountCondition(col,alias));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "disctinct count",col,alias));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggDistinctCountCondition(col,alias));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "disctinct count",col,alias));
		return this;
	}

	@Override
	public <T> T getEntity(Class<T> eClass){
		throw new RuntimeException("还不支持");
		//return null;
	}
	
	@Override
	public SimpleGroupByTable setAliasMap(String col, String alias) {
		if(null == this.selAliasMap){
			this.selAliasMap = new HashMap<String, String>();
		}
		this.selAliasMap.put(col, alias);
		return this;
	}
	
	@Override
	public List<Map<String,Object>> getListMap() {
		List<Map<String,Object>> datalist = null;
		try {
			SearchResponse searchResponse = query();
			Aggregations data = searchResponse.getAggregations();
			datalist = new LinkedList<>();
			ResolveResponse.getSimpleSearchData(data,null,datalist,this.selAliasMap);
		} catch (Exception e) {
			logger.error("",e);
			return null;
		}
		return datalist;
	}
	
	@Override
	public List<Map<String,Object>> getListMap(String path) {
		List<Map<String,Object>> datalist = null;
		try {
			SearchResponse searchResponse = query();
			Aggregations data = searchResponse.getAggregations();
			datalist = new LinkedList<>();
			ResolveResponse.getSimpleSearchDataByPath(data,null,datalist,this.selAliasMap,path);
		} catch (Exception e) {
			logger.error("",e);
			return null;
		}
		return datalist;
	}

	@Override
	public List<Map<String, Object>> getListMap(int from, int to) {
		if(from > to){
			throw new RuntimeException("from larger than to");
		}
		List<Map<String,Object>> resMapList = getListMap();
		int len = resMapList.size();
		if(from > len - 1){
			return new LinkedList<>();
		}
		if(len < to){
			return resMapList.subList(from, len);
		}
		return resMapList.subList(from, to);
	}
	
	@Override
	public List<Map<String, Object>> getListMap(PageHelper helper) {
		Objects.requireNonNull(helper);
		List<Map<String,Object>> resMapList = getListMap();
		helper.setTotalSize(resMapList.size());
		return resMapList.subList(helper.From(), helper.To());
	}

	@Override
	public <T> List<T> getEntityList(Class<T> eClass) {
		List<Map<String,Object>> resMapList = getListMap();
		return getEntitys(eClass,resMapList);
	}
	
	@Override
	public <T> List<T> getEntityList(Class<T> eClass, int from, int to) {
		List<Map<String,Object>> resMapList = getListMap(from, to);
		return getEntitys(eClass,resMapList);
	}
	
	@Override
	public <T> List<T> getEntityList(Class<T> eClass, PageHelper helper) {
		Objects.requireNonNull(helper);
		List<Map<String,Object>> resMapList = getListMap().subList(helper.From(), helper.To());
		helper.setTotalSize(resMapList.size());
		return getEntitys(eClass,resMapList);
	}
	
	private <T> List<T> getEntitys(Class<T> eClass,List<Map<String,Object>> resMapList){
		int len = resMapList.size();
		List<T> dataList = new LinkedList<T>();
		if(len > 0){
			BeanCopierUtil bcu = new BeanCopierUtil();
			Iterator<Map<String,Object>> iterator = resMapList.iterator();
			while(iterator.hasNext()){
				T p = bcu.mapToObject(iterator.next(), eClass);
				dataList.add(p);
			}
		}
		return dataList;
	}
	
	@Override
	public double getSingleResult(String key) {
		try {
			SearchResponse searchResponse = query();
			Aggregations data = searchResponse.getAggregations();
			Iterator<Aggregation> ir = data.iterator();
			Aggregation temp;
			while(ir.hasNext()){
				temp = ir.next();
				if(temp instanceof SingleValue){
					SingleValue sv = (SingleValue) temp;
					if(sv.getName().equals(key)){
						return sv.value();
					}
				}
			}
		} catch (Exception e) {
			logger.error("", e);
		}
		return 0.0;
	}
	
	public SearchResponse query() throws Exception{
    	if(StringUtils.isEmpty(dataBase)){
    		throw new DataBaseNameNullException("库名不能为空");
    	}
    	
    	this.bodyCondition = new BodyCondition();
    	
    	SearchRequest sr = new SearchRequest(dataBase);
    	if(!StringUtils.isEmpty(table)){
    		sr.types(table);
    	}
    	this.bodyCondition.setNumOfRecords(0);
    	this.bodyCondition.addQueryCondtion(this.boolCondition);
    	this.bodyCondition.addAggCondition(this.aggCondition);
    	this.bodyCondition.addRootAggConditions(this.rootAggConditionList);
    	//this.bodyCondition.addRootPipleAggConditions(this.rootPipleAggConditionList);

		sr.source(this.bodyCondition.getCondition());
		
		SearchResponse searchResponse = null;
		try {
			logger.debug("[ES Restful Request]: " + sr.toString());
			RestHighLevelClient client = getClient();
			searchResponse = client.search(sr,RequestOptions.DEFAULT);
			logger.debug("[ES Response]:" + searchResponse);
		} catch (Exception e) {
			logger.error(e.getLocalizedMessage(),e);
		} finally {
			returnBack();
		}
		return searchResponse;
	}

	@Override
	public SimpleGroupByTable Sum(String col, String alias, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,alias,script));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "sum",col,alias,script));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,alias,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "sum",col,alias,script));
		return this;
	}

	@Override
	public SimpleGroupByTable Sum(String col, String alias, String script, Map<String, Object> params) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,alias,script,params));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "sum",col,alias,script));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,alias,script,params));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "sum",col,alias,script));
		return this;
	}

	@Override
	public SimpleGroupByTable Avg(String col, String alias, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,alias,script));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "avg",col,alias,script));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,alias,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "avg",col,alias,script));
		return this;
	}

	@Override
	public SimpleGroupByTable Avg(String col, String alias, String script, Map<String, Object> params) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,alias,script,params));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "avg",col,alias,script));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,alias,script,params));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "avg",col,alias,script));
		return this;
	}

	@Override
	public SimpleGroupByTable Max(String col, String alias, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,alias,script));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "max",col,alias,script));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,alias,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "max",col,alias,script));
		return this;
	}

	@Override
	public SimpleGroupByTable Max(String col, String alias, String script, Map<String, Object> params) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,alias,script,params));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "max",col,alias,script));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,alias,script,params));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "max",col,alias,script));
		return this;
	}

	@Override
	public SimpleGroupByTable Min(String col, String alias, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,alias,script));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "min",col,alias,script));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,alias,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "min",col,alias,script));
		return this;
	}

	@Override
	public SimpleGroupByTable Min(String col, String alias, String script, Map<String, Object> params) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,alias,script,params));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s perform %s", "min",col,alias,script));
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,alias,script,params));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s perform %s", "min",col,alias,script));
		return this;
	}

	@Override
	public SimpleGroupByTable AggCalc(String alias, String script, String...aggCols) {
		if(null == this.aggCondition){
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addPipeLineAgg(new AggBucketScriptCondition(alias, script, aggCols));
		logger.debug(String.format("add a %s agg operation : alias %s perform %s", "BucketScript",alias,script));
		return this;
	}

	@Override
	public <T> SimpleGroupByTable Having(String aggCol, LogicRelation relation, T value) {
		if(null == this.aggCondition){
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addPipeLineAgg(new AggBucketSelectCondition<T>(AggBucketSelectCondition.DEFAULT_ALIAS, aggCol, relation,value));
		logger.debug(String.format("add a %s agg operation : alias %s perform", "Having",aggCol));
		return this;
	}

	@Override
	public SimpleGroupByTable OrderGroup(String aggCol, boolean isDesc) {
		if(null == this.aggCondition){
			return this;
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addPipeLineAgg(new AggBucketSortCondition(aggCol, isDesc));
	
		logger.debug(String.format("add a %s agg operation : alias %s perform %s", "OrderGroup",aggCol,isDesc+""));
		return this;
	}

	@Override
	public SimpleGroupByTable OrderGroup(String[] aggCols, boolean[] isDescs) {
		if(null == this.aggCondition){
			return this;
		}
		int len = aggCols.length;
		Map<String,String> paths = new HashMap<String,String>();
		for(int i =0; i < len; i++){
			paths.put(aggCols[i], aggCols[i]);
		}
		AggBucketLevelCondition temp = (AggBucketLevelCondition)this.aggCondition;
		temp.addPipeLineAgg(new AggBucketSortCondition(aggCols, isDescs));
	
		logger.debug(String.format("add a %s agg operation", "OrderGroup Array"));
		return this;
	}

	@Override
	public SimpleGroupByTable AggTopSum(String alias, String path) {
		if(null == this.aggCondition){
			return this;
		}
		AggBucketLevelCondition bucket = (AggBucketLevelCondition)this.aggCondition;
		bucket.addPipeLineAgg(new AggTopSumCondition(alias,path));
		return this;
	}

	@Override
	public SimpleGroupByTable AggTopAvg(String alias, String path) {
		if(null == this.aggCondition){
			return this;
		}
		AggBucketLevelCondition bucket = (AggBucketLevelCondition)this.aggCondition;
		bucket.addPipeLineAgg(new AggTopAvgCondition(alias,path));
		return this;
	}

	@Override
	public SimpleGroupByTable AggTopMax(String alias, String path) {
		if(null == this.aggCondition){
			return this;
		}
		AggBucketLevelCondition bucket = (AggBucketLevelCondition)this.aggCondition;
		bucket.addPipeLineAgg(new AggTopMaxCondition(alias,path));
		return this;
	}

	@Override
	public SimpleGroupByTable AggTopMin(String alias, String path) {
		if(null == this.aggCondition){
			return this;
		}
		AggBucketLevelCondition bucket = (AggBucketLevelCondition)this.aggCondition;
		bucket.addPipeLineAgg(new AggTopMinCondition(alias,path));
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereAnd(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, vs);
		logger.debug(String.format("add %s condition: %s","and", k, vs));
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereAnd(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, vs);
		logger.debug(String.format("add %s condition: %s","and", k, vs));
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereOr(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, vs);
		logger.debug(String.format("add %s condition: %s","or", k, vs));
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereOr(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, vs);
		logger.debug(String.format("add %s condition: %s","or", k, vs));
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereNot(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, vs);
		logger.debug(String.format("add %s condition: %s","not", k, vs));
		return this;
	}

	@Override
	public <T> SimpleGroupByTable WhereNot(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, vs);
		logger.debug(String.format("add %s condition: %s","not", k, vs));
		return this;
	}

	@Override
	public <T> IWhere OrRange(String k, LogicRelation relation, T to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> IWhere OrRange(String k, T from, T to) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> IWhere OrRange(String k, T from, T to, String timeZone) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SimpleGroupByTable WhereAndPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndPrefix(k, v);
		logger.debug(String.format("add %s Prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereAndWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereAndRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereAndFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereOrPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrPrefix(k, v);
		logger.debug(String.format("add %s prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereOrWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereOrRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereOrFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}
	public SimpleGroupByTable WhereNotPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotPrefix(k, v);
		logger.debug(String.format("add %s Prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereNotWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereNotRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public SimpleGroupByTable WhereNotFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

}
