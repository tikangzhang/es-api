package com.laozhang.entrance;

import org.apache.log4j.Logger;

import com.laozhang.es.base.client.ClientDriver;

public class NormalSearchTest2 {
	public static Logger logger = Logger.getLogger(NormalSearchTest2.class);
	
	public static void main(String[] args) throws Exception {
//		SimpleTable sse = new SimpleTable("cncdev_state","cncdevState");
//		DemoProduct p = sse.WhereAnd("salerNo", "laozhang").From(0).Size(1).OrderBy("logtime", true).getEntity(DemoProduct.class);
//		logger.info("result:" + p);
		
		
//		SimpleTable se = new SimpleTable("cncdev_alarm_detail","cncdevAlarmDetail");
//		List<Map<String,Object>> plist = 
//				se.WhereRange("logTime", "2019-03-06 07:00:00", "2019-03-06 08:00:00")
//				.OrderBy("logTime", true)
//				.getMapList();
//		
//		logger.error(plist);
		
	
		
//		SimpleTable se = new SimpleTable("cncdev_alarm_detail","cncdevAlarmDetail");
//		List<Map<String,Object>> plist = se.WhereAnd("areaName", "A区").WhereRange("logTime", LogicRelation.Gte, "2019-03-06 07:35:00")
//		.Size(10)
//		.SelectCols("logTime","cncdevIp","logTimeDiffer")
//		.OrderBy("cncdevIp", false).OrderBy("logTime", false)
//		.getMapList();
//
//		RangeCondition rc2 = new RangeCondition("logTime");
//		rc2.Gt("2019-03-06 07:35:00");
//		logger.info(plist.size() + "result:" + plist);
		
//		SimpleTable sase = new SimpleTable("cims_user_rank", "userRank");
//	    List<Map<String, Object>> c = sase.WhereAnd("userName","朱文來").getMapList();
//	    logger.error(c);
	    

//	    SimpleTable sase = new SimpleTable("total_tool_depot_manage", "totalToolDepotManage");
//
//	     sase.WhereIsNotNull("reportDay")
//	            .WhereIsNotNull("toolCarBindingTime")
//	            .WhereAnd("reportDay",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2019-08-07 00:00:00").getTime());
//	     List<TotalToolDepotManage> s = sase.OrderBy("groupName",false)
//	    		 .OrderBy("differ",false)
//	    		 .Size(1000)
//	    		 .getEntityList(TotalToolDepotManage.class);
	    
//	     SimpleGroupByTable sase = new SimpleGroupByTable("total_tool_depot_manage", "totalToolDepotManage");
//	     sase.WhereIsNotNull("toolCarBindingTime")
//	             .WhereIsNotNull("reportDay")
//	             .WhereAnd("reportDay",new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2019-08-07 00:00:00").getTime());
//
//	     List<Map<String,Object>> s  =sase.GroupBy("groupName").OrderByGroupName(false)
//	             .Count("groupName","number")
//	             .SelectAggCols("groupName").EndSelectAggCols()
//	             .getListMap();
//	    logger.info(s);
		ClientDriver.Destroy();
		
//		Class<?> clazz = TotalToolDepotManage.class;
//		Method method = clazz.getMethod("setId", new Class<?>[]{String.class});
//		Object o = clazz.newInstance();
//		method.invoke(o, "xxxxxy");
		
	}
}
