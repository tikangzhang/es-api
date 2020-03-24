package com.laozhang.entrance;

import java.util.List;
import java.util.Map;

import com.laozhang.es.oper.search.simple.SimpleGroupByTable;
import org.apache.log4j.Logger;

import com.laozhang.es.base.client.ClientDriver;

public class AggsSearchTest2 {
	public static Logger logger = Logger.getLogger(AggsSearchTest2.class);
	
	public static void main(String[] args) {
		List<Map<String,Object>> data = null;
		SimpleGroupByTable se = null;
		long now = System.currentTimeMillis();
		se = new SimpleGroupByTable("cncdev_alarm_detail","cncdevAlarmDetail");
		data = se.WhereRange("logTime", "2019-08-15 08:00:00", "2019-08-20 08:00:00")
				.GroupBy("procedureName")
				.AggTopSum("alarmCount", "alarmId>alarmOne")
				.GroupBy("alarmId")
			    .Max("cncdevIp","alarmOne","return 1")
			    .getListMap("alarmCount");
		logger.info("result:" + data);
		logger.info(System.currentTimeMillis() - now);
//		se = new SimpleGroupByTable("noneid_production_capacity","productCount");
//		data = se.WhereRange("logTime", "2018-09-18 08:00:00", "2019-03-18 08:00:00")
//				.GroupBy("cncdevName")
//				.GroupBy("lineName")
//				.Sum("productCountDiffer","diff_sum")
//				.Avg("productCountDiffer","avg_sum")
//				.getListMap();
//		logger.info("result:" + data);
//		
//		se = new SimpleGroupByTable("noneid_production_capacity","productCount");
//		data = se.WhereRange("logTime", "2018-09-18 08:00:00", "2018-11-01 08:00:00")
//				.GroupBy("procedureName")
//				.GroupBy("lineName")
//				.SelectAggCols("procedureName","logTime")
//				.Sum("productCountDiffer","diff_sum")
//				.Avg("productCountDiffer", "avg_sum")
//				.getListMap();
//		logger.info("result:" + data);
//		
//		se = new SimpleGroupByTable("noneid_production_capacity","productCount");
//		data = se.WhereRange("logTime", "2018-09-18 08:00:00", "2018-11-01 08:00:00")
//				  .GroupBy("lineName")
//				  .GroupBy("procedureName")
//				  .SelectAggCols("procedureName","lineName","logTime")
//				    .Sum("productCountDiffer","diff_sum")
//				    .Avg("productCountDiffer", "avg_sum")
//				    .OrderBy("diff_sum", false)
//				    .OrderBy("avg_sum", true)
//				    .getListMap();
//		logger.info("result:" + data);
//		
//		se = new SimpleGroupByTable("noneid_production_capacity","productCount");
//		String script = "if(doc.lineName.value == 'A'){return doc.productCountDiffer.value}else{return 0}";
//		data = se.WhereRange("logTime", "2018-09-18 08:00:00", "2018-11-01 08:00:00")
//			.GroupBy("procedureName")
//			.GroupBy("groupTime2h")
//			.SelectAggCols("procedureName","groupTime2h","logTime")
//			.Sum("productCountDiffer","diff_sum")
//			.Sum("productCountDiffer","diff_sum_script",script)
//		    .Avg("productCountDiffer", "avg_sum")
//		    .getListMap();
//		logger.info("result:" + data);
//		
//		se = new SimpleGroupByTable("noneid_production_capacity","productCount");
//		data = se.WhereRange("logTime", "2018-09-18 08:00:00", "2018-11-01 08:00:00")
//			    .GroupBy("procedureName")
//			    .DistinctCount("cncdevName", "machineNo_count")
//			    .Sum("productCountDiffer","diff_sum_f")
//			    .GroupBy("groupTime2h")
//			    .SelectAggCols("procedureName","groupTime2h","logTime")
//			    .Sum("productCountDiffer","diff_sum")
//			    .Sum("productCountDiffer","diff_sum_script",script)
//			    .Avg("productCountDiffer", "avg_sum")
//			    .getListMap();
//		logger.info("result:" + data);
		ClientDriver.Destroy();
	}
}
