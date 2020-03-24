package com.laozhang.entrance;

import java.util.List;
import java.util.Map;

import com.laozhang.es.base.condition.common.Arithmetic;
import com.laozhang.es.oper.search.time.TimePeriodGroupByTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimePeriodStatSearchTest {
	public static Logger logger = LoggerFactory.getLogger(TimePeriodStatSearchTest.class);
	
	public static void main(String[] args) {
		List<Map<String,Object>> data = null;
		
		/*
		 	筛选条件与SimpleAggsSearchExecutor相同
		 	以天为间隔统计
		 */
		TimePeriodGroupByTable tsse = new TimePeriodGroupByTable("product","productCount");
		data = tsse
				.WhereRange("logtime", "2018-09-13T08:00:00", "2018-09-15T08:00:00")
				.StatByDay("logtime")
				.SelectAggCols("producterNo","machineNo","logtime")
				.setMainKey("myKey")
				.Sum("logtimediff","devs_output")
				.Sum("logtimediff","logtimediffsumdivide",Arithmetic.Divide,3)
				.Count("logtimediff","logtimediffcount")
				.getListMap();
		logger.info("result:"+data);
		
	}
}
