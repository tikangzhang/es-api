package com.laozhang.entrance;

import java.util.List;
import java.util.Map;

import com.laozhang.es.oper.search.simple.SimpleGroupByTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BatchExport {
	public static Logger logger = LoggerFactory.getLogger(BatchExport.class);
	
	public static void main(String[] args) throws Exception {

		List<Map<String,Object>> data = null;
		SimpleGroupByTable se = null;
		long now = System.currentTimeMillis();
		se = new SimpleGroupByTable("cncdev_alarm_detail","cncdevAlarmDetail");
		data = se
				.WhereAnd("alarmNo","")

				.WhereRange("logTime", "2019-08-20 08:00:00", "2019-08-21 08:00:00")
				.GroupBy("alarmId")
				.getListMap();
//		logger.info("result:" + data);
		logger.info("{}",System.currentTimeMillis() - now);
	}
}
