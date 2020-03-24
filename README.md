基于Elasticsearch 6.4.3 高级接口封装
以类sql的api查询数据

例如：
		SimpleGroupByTable se = new SimpleGroupByTable("xxxIndex","yyyType");
		List<Map<String,Object>> data = se.WhereAnd("fieldA","value")
				.GroupBy("name")
				.Sum("v","sumOfv")
				.getListMap();
        
    System.out.println("result:" + data);
		ClientDriver.Destroy();
