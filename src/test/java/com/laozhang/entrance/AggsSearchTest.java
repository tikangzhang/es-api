package com.laozhang.entrance;

import java.util.List;
import java.util.Map;

import com.laozhang.es.base.client.ClientDriver;
import com.laozhang.es.oper.search.simple.SimpleGroupByTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggsSearchTest {
	public static Logger logger = LoggerFactory.getLogger(AggsSearchTest.class);
	
	public static void main(String[] args) {
		List<Map<String,Object>> data = null;
		SimpleGroupByTable se = null;
		
		/*
			select sum(logtimediff) as diff_sum from product.productCount 
			where producterNo = 'NO01' and logtime >= '2018-08-18T16:58:01' and logtime < '2018-08-22T16:58:01'
			group by machineNo,salerNo
		*/
//		se = new SimpleGroupByTable("product","productCount");
//		data = se.WhereAnd("producterNo", "NO01")
//				.WhereRange("logtime", "2018-08-18T16:58:01", "2018-08-22T16:58:01")
//				.GroupBy("machineNo","salerNo")
//				.Sum("logtimediff","diff_sum")
//				.getListMap();
//		logger.info("result:" + data);
		
		/*超越sql只能聚合小组的限制，一条请求 同时聚合父组 跟子组，并支持多深度分组聚合
			select Count(Distinct machineNo) as machineNo,sum(logtimediff) as machineNo from product.productCount 
			where logtime >= '2018-08-18T16:58:01' and logtime < '2018-08-22T16:58:01'
			group by producterNo (父组聚合)
			
			union all(以列并集)
			
			select producterNo,machineNo,logtime,sum(logtimediff) as diff_sum,avg(logtimediff) as diff_avg from product.productCount 
			where logtime >= '2018-08-18T16:58:01' and logtime < '2018-08-22T16:58:01'
			group by producterNo,machineNo (子组聚合)
		*/
//		se = new SimpleGroupByTable("product","productCount");
//		data = se.WhereRange("logtime", "2018-08-18T16:58:01", "2018-08-22T16:58:01")
//				.GroupBy("producterNo")
//				.DistinctCount("machineNo", "machineNo_count")
//				.Sum("logtimediff", "producter_diff_sum")
//				.GroupBy("machineNo")
//				.SelectAggCols("producterNo","machineNo","logtime")
//				.Sum("logtimediff","diff_sum")
//				.Avg("logtimediff","diff_avg")
//				.getListMap();
//		logger.info("result:" + data);
		
		
		/*
		 	select t.producterNo,t.machineNo,t.logtime,sum(t.logtimediff) as diff_sum,avg(t.logtimediff) as diff_avg from 
		 	(
		 		select * from product.productCount 
		 		where logtime >= '2018-08-18T16:58:01' and logtime < '2018-08-22T16:58:01'
		 		order by logtime desc 
		 	) t
			group by t.producterNo,t.machineNo 
		 */
//		se = new SimpleGroupByTable("cncdev_state","cncdevState");
//		data = se.WhereRange("logTime", "2019-03-06 07:35:00", "2019-03-07 08:00:00")
//				.AggTopSum("all_sum", "time_id>haha")
//				.GroupBy("groupTime2h","time_id")
//				.SelectAggCols("groupTime2h","logTime")
//			.Sum("logTimeDiffer","diff_sum")
//			.AggCalc("haha", "BigDecimal.valueOf((diff_sum + 1155) / 10000.0).setScale(2, RoundingMode.HALF_UP).doubleValue()", "diff_sum")
//			.getListMap();
//		logger.info("result:" + data);
		
		
//		se = new SimpleGroupByTable("cncdev_state","cncdevState");
//		data = se.WhereRange("logTime", "2019-03-06 07:35:00", "2019-03-07 08:00:00")
//				.GroupBy("groupTime2h","time_id")
//				.SelectAggCols("groupTime2h","logTime")
//				.AppendAggScriptCol("type", "return '老张'")
//				.AppendAggScriptCol("xxx", "return 'fuc'")
//				.EndSelectAggCols()
//			.Sum("logTimeDiffer","diff_sum")
//			.getListMap();
//		logger.info("result:" + data);
		
		
//		double result = (double)se.WhereRange("logTime", "2019-03-06 07:35:00", "2019-03-06 08:00:00")
//				.Sum("logTimeDiffer","productivity")
//				.getSingleResult("productivity");
//		long timeCount = 10000L;
//		logger.info("result:" + String.format("%.2f", (timeCount - result / timeCount)));
//		String colNameScript = "String temp = doc.procedureName.value + ' ' + doc.lineName.value + ' cell ';if (doc.buildingFloorNo.value != null){return  temp + doc.buildingFloorNo.value + '機群 op待料'} else {return temp;}";
//		String script = "return 1";
//		se = new SimpleGroupByTable("cncdev_state","cncdevState");
//		data = se.WhereRange("logTime", "2019-03-06 07:35:00", "2019-03-06 08:00:00")
//			.GroupBy("groupTime2h")
//			.Sum("areaName", "counts", script)
//			.Sum("logTimeDiffer","diff_sum")
//			.AggCalc("othercalc", "diff_sum/counts*100", "counts","diff_sum")
//			.AggCalc("othercalc++", "othercalc / 10000", "othercalc")
//			.SelectAggCustomCols("type",colNameScript,"procedureName","lineName","buildingFloorNo")
//			.getListMap();
//		logger.info("result:" + data);
		
		
		/*
			select count(machineNo) as diff_count,sum(logtimediff) as diff_sum,sum(logtimediff * 5) as diff_sum5 from product.productCount 
			where producterNo = 'NO01' and logtime >= '2018-08-18T16:58:01' and logtime < '2018-08-22T16:58:01'
			group by machineNo
			order by diff_sum asc,diff_count desc
		*/
//		se = new SimpleGroupByTable("product","productCount");
//		data = se.WhereAnd("producterNo", "NO01")
//				.WhereRange("logtime", "2018-08-18T16:58:01", "2018-08-22T16:58:01")
//				.GroupBy("machineNo")
//				.Count("machineNo","diff_count")
//				.Sum("logtimediff", "diff_sum")
//				.Sum("logtimediff", "diff_sum5",Arithmetic.Multiply,5)
//				.OrderBy("diff_sum", false)
//				.OrderBy("diff_count", true)
//				.getListMap();
//		logger.info("result:" + data);
		
		
		/*
		  	select 
		  	machineNo ,
		  	logtime ,
		  	case when machineNo == 'A02' then 'haha' else 'xxxx' as colNameScript,
		  	sum(logtimediff) as diff_sum,
		  	sum(
		  		case when (machineNo == 'A02') then logtimediff else 0
		  	) as diff_sum_script 
		  	from product.productCount 
			where salerNo = '老张' and logtime >= '2018-08-21T08:00:00' and logtime < '2018-08-23T08:00:00'
			group by machineNo
		 */
//		String script = "if(doc.machineNo.value == 'A02'){return doc.logtimediff.value}else{return 0}";
//		String colNameScript = "if(doc.machineNo.value == 'A02'){return 'haha';}else{return 'xxxx'}";
//		se = new SimpleGroupByTable("product","productCount");
//		data = se.WhereAnd("salerNo", "老张")
//				.WhereRange("logtime", "2018-08-21T08:00:00", "2018-08-23T08:00:00")
//				.GroupBy("machineNo")
//				.Sum("logtimediff","diff_sum")
//				.Sum("logtimediff","diff_sum_script",script)
//				.SelectOrderedAggCustomCols("colNameScript",colNameScript,"logtime",true,"machineNo","logtime")
//				.getListMap();
//		logger.info("result:" + data);

//		Map<String,Object> params = new HashMap<>();
//		params.put("var1","1-I25");
//		String script = "if(doc.cncdevName.value == params.var1){return doc.productCountDiffer.value}else{return 0}";
//		se = new SimpleGroupByTable("production_capacity","productionCapacity");
//		data = se.WhereRange("logTime", "2019-11-01 00:00:00", "2019-11-01 08:00:00").WhereAnd("cncdevName","1-I25")
//				.GroupBy("cncdevName")
//				.SelectAggCols("cncdevName").setAliasMap("cncdevName", "state").EndSelectAggCols()
//				.Sum("productCountDiffer","sumOfCount",script,params)
//				.getListMap();
//		logger.info("result:" + data);
		
		
//		se = new SimpleGroupByTable("cims_rank_detail","rankDetail");
//		data = se.WhereAnd("lineName", "B").WhereAnd(new BoolCondition().And("groupName", "日铭").Or("userName", "毛建明"))
//				.GroupBy("procedureName")
//				.Count("lineName","total_count")
//				.getListMap();
//		logger.info("result:" + data);
		/*
		select producterNo,machineNo,desc,logtime,
		avg(logtimediff) as avg_sum,
		sum(logtimediff) as diff_sum 
		from product.productCount 
		where salerNo='laozhang' and logtime >='2018-09-13T08:00:00' and logtime < '2018-09-14T08:00:00' 
		group by producterNo,machineNo
		order by diff_sum asc,avg_sum desc
		*/
//		se = new SimpleGroupByTable("product","productCount");
//		data = se.WhereAnd("salerNo", "laozhang")
//				.WhereRange("logtime", "2018-09-18T08:00:00", "2018-09-19T08:00:00",CommonConstant.COMMON_EAST8_TIME_ZONE)
//				.GroupBy("producterNo")
//				.GroupBy("machineNo")
//				.SelectAggCols("producterNo","machineNo","desc","logtime")
//				.Sum("logtimediff","diff_sum")
//				.Avg("logtimediff", "avg_sum")
//				.OrderBy("diff_sum", false)
//				.OrderBy("avg_sum", true)
//				.getListMap();
//		logger.info("result:" + data);

		se = new SimpleGroupByTable("material_binding","productionDayReport");
		data = se.WhereAnd("factory1","凯胜")
				.GroupBy("factory2").OrderByGroupName(true)
				.GroupBy("factory3").OrderByGroupName(false)
				.GroupBy("factory6").OrderByGroupName(true)
				.Sum("output","sumOfCount")
				.getListMap();
		logger.info("result:" + data);

		ClientDriver.Destroy();
	}
}
