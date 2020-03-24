package com.laozhang.entrance;

import com.laozhang.es.oper.crud.IDeleteByQuery;
import com.laozhang.es.oper.crud.impl.DeleteByQuery;

public class TestLowLevelClient {

	public static void main(String[] args) {
		DeleteByQuery executor = new DeleteByQuery("","");
		executor.delete("").responseToJson();
		
		
		String ster = new DeleteByQuery("","").delete("").getJson();
		IDeleteByQuery query = new DeleteByQuery("","");
		query.delete("").getJson();
		
	} 
}
