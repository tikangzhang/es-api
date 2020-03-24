package com.laozhang.es.oper.search.time;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.laozhang.es.base.CommonConstant;
import com.laozhang.es.base.condition.BoolCondition;
import com.laozhang.es.base.Executor;
import com.laozhang.es.base.condition.BodyCondition;
import com.laozhang.es.base.condition.RangeCondition;
import com.laozhang.es.base.condition.aggs.*;
import com.laozhang.es.base.condition.common.Arithmetic;
import com.laozhang.es.base.condition.common.LogicRelation;
import com.laozhang.es.base.exception.DataBaseNameNullException;
import com.laozhang.es.base.utils.BeanCopierUtil;
import com.laozhang.es.oper.search.*;
import com.laozhang.es.base.utils.page.PageHelper;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.ParsedDateRange;
import org.elasticsearch.search.aggregations.bucket.range.Range.Bucket;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeRangeGroupByTable extends Executor implements ITimeRangeGroupByTable, IWhere, IGroupByForDateRange, IResult, IPipGroupBy {
	
	public static Logger logger = LoggerFactory.getLogger(TimeRangeGroupByTable.class);
			
	private String dataBase;
	
	private String table;
	
	private BodyCondition bodyCondition;
	
	private BoolCondition boolCondition;
	
	private RangeCondition rangCondition;
	
	private AggCondition aggCondition;
	
	private LinkedList<AggCondition> rootAggConditionList = new LinkedList<>();
	
	private LinkedList<AggCondition> rootPipleAggConditionList = new LinkedList<>();
	
	private String key = "_key";
	
	public TimeRangeGroupByTable(String dataBaseName){
		this(dataBaseName,null);
	}
	
	public TimeRangeGroupByTable(String dataBaseName, String tableName){
		this.dataBase = dataBaseName;
		this.table = tableName;
	}
	
	@Override
	public TimeRangeGroupByTable SelectAggCols(String...cols) {
		if(null != this.aggCondition) {
			logger.debug("select:" + cols);
			AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
			temp.top(1, null, false, cols);
		}
		
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable SelectOrderedAggCols(String orderedCol, boolean desc, String...cols) {
		if(null != this.aggCondition) {
			logger.debug("select ordered cols:" + cols);
			AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
			temp.top(1, orderedCol, desc, cols);
		}
		
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereAnd(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable Where(BoolCondition bc) {
		boolCondition = bc;
		logger.debug("add a sub condition");
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereOr(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","or", k,String.valueOf(v)));
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereNot(String k, T v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, v);
		logger.debug(String.format("add %s condition: %s -> %s","not", k,String.valueOf(v)));
		return this;
	}
	
	@Override
	public <T> TimeRangeGroupByTable WhereAnd(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubAnd(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereOr(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubOr(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereNot(BoolCondition condition) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.addSubNot(condition);;
		logger.debug("add a composite condition");
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereRange(String k, Object from, Object to) {
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
	public <T> TimeRangeGroupByTable WhereRange(String k, LogicRelation relation, T to) {
		if(LogicRelation.Ne.equals(relation)){
			logger.error("do not support Ne relation");
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
	public TimeRangeGroupByTable WhereRange(String k, Object from, Object to, String timeZone) {
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
	public TimeRangeGroupByTable StatBy(String col){
		if(StringUtils.isEmpty(col)){
			return this;
		}
		AggTimeRangeStatCondition atrsc = new AggTimeRangeStatCondition(col);
		atrsc.setFormater(CommonConstant.DEFAULT_TIME_KEY_FORMATER);
		this.aggCondition = atrsc;
		logger.debug("stat by" + col);
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
		boolCondition.IsNotNull(k);;
		logger.debug(String.format("add a condition: %s is not null",k));
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable AddStart(String Start, String alias) {
		if(StringUtils.isEmpty(alias)){
			return this;
		}
		if(null != this.aggCondition){
			AggTimeRangeStatCondition atrsc = (AggTimeRangeStatCondition)this.aggCondition;
			atrsc.addStart(Start, alias);
			logger.debug("add a start:" + Start);
		}
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable AddEnd(String End, String alias) {
		if(StringUtils.isEmpty(alias)){
			return this;
		}
		if(null != this.aggCondition){
			AggTimeRangeStatCondition atrsc = (AggTimeRangeStatCondition)this.aggCondition;
			atrsc.addEnd(End, alias);
			logger.debug("add a end:" + End);
		}
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable AddRange(String Start, String End, String alias) {
		if(StringUtils.isEmpty(alias)){
			return this;
		}
		if(null != this.aggCondition){
			AggTimeRangeStatCondition atrsc = (AggTimeRangeStatCondition)this.aggCondition;
			atrsc.addRang(Start, End, alias);
			logger.debug("add a range:" + Start + " -> " + "End");
		}
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable Sum(String col) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s", "sum",col));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,col));
		logger.debug(String.format("add a metrics %s agg operation : col name %s", "sum",col));
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable Sum(String col, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s", "sum",col,opType.getName(),String.valueOf(opNum)));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,col,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s", "sum",col,opType.getName(),String.valueOf(opNum)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable Avg(String col) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s with alias %s", "avg",col,col));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,col));
		logger.debug(String.format("add a metrics %s agg operation : col name %s with alias %s", "avg",col,col));
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable Avg(String col, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s", "avg",col,opType.getName(),String.valueOf(opNum)));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,col,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s", "avg",col,opType.getName(),String.valueOf(opNum)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable Max(String col) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s", "max",col));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,col));
		logger.debug(String.format("add a metrics %s agg operation : col name %s", "max",col));
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable Max(String col, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s", "max",col,opType.getName(),String.valueOf(opNum)));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,col,opType,opNum));
		logger.debug(String.format("add a metrics %s agg operation : col name %s %s %s", "max",col,opType.getName(),String.valueOf(opNum)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable Min(String col) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s", "min",col));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,col));
		logger.debug(String.format("add a metrics %s agg operation : col name %s", "min",col));
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable Min(String col, Arithmetic opType, Object opNum) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s %s %s", "min",col,opType.getName(),String.valueOf(opNum)));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,col,opType,opNum));
		logger.debug(String.format("add a root %s agg operation : col name %s %s %s", "min",col,opType.getName(),String.valueOf(opNum)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable Count(String col) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggCountCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s", "count",col));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggCountCondition(col,col));
		logger.debug(String.format("add a metrics %s agg operation : col name %s", "count",col));
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable DistinctCount(String col) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggDistinctCountCondition(col,col));
			logger.debug(String.format("add a root %s agg operation : col name %s", "disctinct count",col));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggDistinctCountCondition(col,col));
		logger.debug(String.format("add a metrics %s agg operation : col name %s", "disctinct count",col));
		return this;
	}
	
	@Override
	public TimeRangeGroupByTable setMainKey(String key) {
		logger.debug("set key:"+key);
		this.key = key;
		return this;
	}

	@Override
	public <T> T getEntity(Class<T> eClass){
		throw new RuntimeException("还不支持");
		//return null;
	}
	
	private void getData(Aggregations aggs,List<Map<String,Object>> refList){
		Aggregations data = aggs;
		
		Map<String,Object> tempMap = new HashMap<String,Object>();
		
		Iterator<Aggregation> ir = data.iterator();
		ParsedDateRange temp = (ParsedDateRange)ir.next();
		
		List<? extends Bucket> list = temp.getBuckets();
		String key;
		for(int i = 0,len = list.size(); i < len; i++){
			Bucket buket= list.get(i);
			key = buket.getKeyAsString();
			Aggregations chiledData = buket.getAggregations();
			if(null != chiledData){
				Iterator<Aggregation> irc = chiledData.iterator();
				SingleValue v = (SingleValue)irc.next();
				tempMap.put(key, v.value());
			}else{
				tempMap.put(key, 0);
			}
		}
		refList.add(tempMap);
	}
	
	private void getData(Aggregations aggs,Map<String,Object> mainMap,List<Map<String,Object>> refList){
		boolean enterSubTerms = false;
		ParsedDateRange next = null;
		Aggregations data = aggs;
		Iterator<Aggregation> ir = data.iterator();
		Map<String,Object> tempMap = new HashMap<String,Object>();
		while(ir.hasNext()){
			Object temp = ir.next();

			if(temp instanceof ParsedDateRange){
				next = ((ParsedDateRange) temp);
				continue;
			}else if(temp instanceof TopHits){
				TopHits th = (TopHits)temp;
				SearchHit[] earchHit = th.getHits().getHits();
				if(null == earchHit || earchHit.length == 0){
					continue;
				}
				Map<String,Object> hitMap = earchHit[0].getSourceAsMap();
				tempMap.putAll(hitMap);
			}else if(temp instanceof SingleValue){
				SingleValue sv = (SingleValue) temp;
				tempMap.put(sv.getName(), sv.value());
			}
		}
		
		if(null != mainMap){
			tempMap.putAll(mainMap);
		}
		
		if(null != next){
			List<? extends Bucket> list = next.getBuckets();
			
			for(int i = 0,len = list.size(); i < len; i++){
				Bucket buket= list.get(i);
				tempMap.put(this.key, buket.getKey().toString());
				data = null;
				data = buket.getAggregations();
				if(null != data){
					enterSubTerms = true;
					getData(data,tempMap,refList);
				}
			}
		}
		
		if(tempMap.size() > 0 && !enterSubTerms){
			refList.add(tempMap);
		}
	}
	
	@Override
	public List<Map<String,Object>> getListMap() {
		List<Map<String,Object>> datalist = null;
		try {
			SearchResponse searchResponse = query();
			Aggregations data = searchResponse.getAggregations();
			datalist = new LinkedList<>();
			getData(data,null,datalist);
		} catch (Exception e) {
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
			ResolveResponse.getSimpleSearchDataByPath(data,null,datalist,null,path);
		} catch (Exception e) {
			logger.error("",e);
			return null;
		}
		return datalist;
	}
	
	
	@Override
	public List<Map<String, Object>> getListMap(int from, int to) {
		List<Map<String,Object>> resMapList = getListMap();
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
		List<Map<String,Object>> resMapList = getListMap().subList(from, to);
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
	
	public List<Map<String,Object>> getFlatListMap() {
		List<Map<String,Object>> datalist = null;
		try {
			SearchResponse searchResponse = query();
			Aggregations data = searchResponse.getAggregations();
			datalist = new LinkedList<>();
			getData(data,datalist);
		} catch (Exception e) {
			return null;
		}
		return datalist;
	}
	
	private SearchResponse query() throws Exception{
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
    	this.bodyCondition.addRootPipleAggConditions(this.rootPipleAggConditionList);
		sr.source(this.bodyCondition.getCondition());
		
		SearchResponse searchResponse = null;
		try {
			logger.debug("[ES Restful Request]: " + sr.toString());
			RestHighLevelClient client = getClient();
			searchResponse = client.search(sr,RequestOptions.DEFAULT);
			logger.debug("[ES Response]:" + searchResponse);
		} catch (IOException e) {
			logger.error(e.getLocalizedMessage(),e);
		} finally {
			returnBack();
		}
		return searchResponse;
	}

	@Override
	public TimeRangeGroupByTable Sum(String col, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggSumCondition(col,col,script));
			logger.debug(String.format("add a root %s agg operation : col name %s perform %s", "sum",col,script));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggSumCondition(col,col,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s perform %s", "sum",col,script));
		return this;
	}

	@Override
	public TimeRangeGroupByTable Avg(String col, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggAvgCondition(col,col,script));
			logger.debug(String.format("add a root %s agg operation : col name %s perform %s", "avg",col,script));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggAvgCondition(col,col,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s perform %s", "avg",col,script));
		return this;
	}

	@Override
	public TimeRangeGroupByTable Max(String col, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMaxCondition(col,col,script));
			logger.debug(String.format("add a root %s agg operation : col name %s perform %s", "max",col,script));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMaxCondition(col,col,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s perform %s", "max",col,script));
		return this;
	}

	@Override
	public TimeRangeGroupByTable Min(String col, String script) {
		if(null == this.aggCondition){
			this.rootAggConditionList.add(new AggMinCondition(col,col,script));
			logger.debug(String.format("add a root %s agg operation : col name %s perform %s", "min",col,script));
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addMetricsAgg(new AggMinCondition(col,col,script));
		logger.debug(String.format("add a metrics %s agg operation : col name %s perform %s", "min",col,script));
		return this;
	}

	@Override
	public TimeRangeGroupByTable SelectAggCustomCols(String alias, String Script, String... cols) {
		throw new RuntimeException("不支持");
	}

	@Override
	public TimeRangeGroupByTable SelectOrderedAggCustomCols(String alias, String Script, String orderedCol, boolean desc,
															String... cols) {
		throw new RuntimeException("不支持");
	}

	@Override
	public TimeRangeGroupByTable setAliasMap(String col, String alias) {
		throw new RuntimeException("不支持");
	}

	@Override
	public TimeRangeGroupByTable AggSize(Integer size) {
		AggTermsCondition temp = (AggTermsCondition)this.aggCondition;
		if(null != temp){
			temp.size(size);
			logger.debug("set limit:" + size);
		}
		return this;
	}

	@Override
	public TimeRangeGroupByTable AggCalc(String alias, String script, String... aggCols) {
		if(null == this.aggCondition){
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addPipeLineAgg(new AggBucketScriptCondition(alias, script, aggCols));
		logger.debug(String.format("add a %s agg operation : alias %s perform %s", "BucketScript",alias,script));
		return this;
	}
	
	@Override
	public <T> TimeRangeGroupByTable Having(String aggCol, LogicRelation relation, T value) {
		if(null == this.aggCondition){
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addPipeLineAgg(new AggBucketSelectCondition<T>(AggBucketSelectCondition.DEFAULT_ALIAS, aggCol, relation,value));
		logger.debug(String.format("add a %s agg operation : alias %s perform", "Having",aggCol));
		return this;
	}

	@Override
	public TimeRangeGroupByTable OrderGroup(String aggCol, boolean isDesc) {
		if(null == this.aggCondition){
			return this;
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addPipeLineAgg(new AggBucketSortCondition(aggCol, isDesc));
	
		logger.debug(String.format("add a %s agg operation : alias %s perform %s", "OrderGroup",aggCol,isDesc+""));
		return this;
	}

	@Override
	public TimeRangeGroupByTable OrderGroup(String[] aggCols, boolean[] isDescs) {
		if(null == this.aggCondition){
			return this;
		}
		int len = aggCols.length;
		Map<String,String> paths = new HashMap<String,String>();
		for(int i =0; i < len; i++){
			paths.put(aggCols[i], aggCols[i]);
		}
		AggTimeRangeStatCondition temp = (AggTimeRangeStatCondition)this.aggCondition;
		temp.addPipeLineAgg(new AggBucketSortCondition(aggCols, isDescs));
	
		logger.debug(String.format("add a %s agg operation", "OrderGroup Array"));
		return this;
	}
	@Override
	public TimeRangeGroupByTable AggTopSum(String alias, String path) {
		this.rootPipleAggConditionList.add(new AggTopSumCondition(alias,path));
		return this;
	}

	@Override
	public TimeRangeGroupByTable AggTopAvg(String alias, String path) {
		this.rootPipleAggConditionList.add(new AggTopAvgCondition(alias,path));
		return this;
	}

	@Override
	public TimeRangeGroupByTable AggTopMax(String alias, String path) {
		this.rootPipleAggConditionList.add(new AggTopMaxCondition(alias,path));
		return this;
	}

	@Override
	public TimeRangeGroupByTable AggTopMin(String alias, String path) {
		this.rootPipleAggConditionList.add(new AggTopMinCondition(alias,path));
		return this;
	}
	
	@Override
	public <T> TimeRangeGroupByTable WhereAnd(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, vs);
		logger.debug(String.format("add %s condition: %s","and", k, vs));
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereAnd(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.And(k, vs);
		logger.debug(String.format("add %s condition: %s","and", k, vs));
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereOr(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, vs);
		logger.debug(String.format("add %s condition: %s","or", k, vs));
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereOr(String k, Collection<?> vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Or(k, vs);
		logger.debug(String.format("add %s condition: %s","or", k, vs));
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereNot(String k, Object... vs) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.Not(k, vs);
		logger.debug(String.format("add %s condition: %s","not", k, vs));
		return this;
	}

	@Override
	public <T> TimeRangeGroupByTable WhereNot(String k, Collection<?> vs) {
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
	public TimeRangeGroupByTable WhereAndPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndPrefix(k, v);
		logger.debug(String.format("add %s Prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereAndWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereAndRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereAndFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.AndFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereOrPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrPrefix(k, v);
		logger.debug(String.format("add %s prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereOrWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereOrRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereOrFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.OrFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}
	public TimeRangeGroupByTable WhereNotPrefix(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotPrefix(k, v);
		logger.debug(String.format("add %s Prefix condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereNotWildcard(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotWildcard(k, v);
		logger.debug(String.format("add %s Wildcard condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereNotRegexp(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotRegexp(k, v);
		logger.debug(String.format("add %s Regexp condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

	@Override
	public TimeRangeGroupByTable WhereNotFuzzy(String k, String v) {
		if(null == boolCondition){
			boolCondition = new BoolCondition();
		}
		boolCondition.NotFuzzy(k, v);
		logger.debug(String.format("add %s Fuzzy condition: %s -> %s","and", k,String.valueOf(v)));
		return this;
	}

}
