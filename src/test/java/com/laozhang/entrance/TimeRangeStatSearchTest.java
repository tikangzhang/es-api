package com.laozhang.entrance;

import com.laozhang.es.oper.search.time.TimeRangeGroupByTable;

import java.util.List;
import java.util.Map;

public class TimeRangeStatSearchTest {
	public static void main(String[] args) {
		List<Map<String,Object>> data = null;
		
		/*
		 	筛选条件与SimpleAggsSearchExecutor相同
		 	按时间范围进行统计
		 */
		TimeRangeGroupByTable tsse = new TimeRangeGroupByTable("product","productCount");
		data = tsse.WhereAnd("salerNo", "老张")
				.WhereRange("logtime", "2018-08-21T08:00:00", "2018-08-22T08:00:00")
				.StatBy("logtime")
				.AddRange("2018-08-21T23:00:00", "2018-08-22T00:00:00", "p23")
				.AddRange("2018-08-22T00:00:00", "2018-08-22T01:00:00", "p00")
				.AddRange("2018-08-22T01:00:00", "2018-08-22T02:00:00", "p01")
				//.SelectAggCols("producterNo","machineNo","logtime")
				//.setMainKey("myKey")
				.Sum("logtimediff")
				.getFlatListMap();
		System.out.println(data);
		
	}
}
