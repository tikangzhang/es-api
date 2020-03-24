package com.laozhang.es.base.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class FunctionUtil {
	/**
	 * List<Map<String, Object>> 专用比较器
	 * arrCols 多列
	 * arrColsOrder 每列升序还是降序
	 */
	public static class ListMapComparator implements Comparator<Map<String, Object>>{
		private String[] arrCols;
		
		private boolean[] arrColsOrder;
		
		public ListMapComparator(String[] arrCols){
			this(arrCols,new boolean[]{});
		}
		
		public ListMapComparator(String[] arrCols,boolean[] arrColsOrder){
			if(arrCols.length != arrColsOrder.length){
				throw new RuntimeException("排序列数跟执行升序数不一致");
			}
			this.arrCols = arrCols;
			this.arrColsOrder = arrColsOrder;
		}
		
		@Override
		public int compare(Map<String, Object> o1, Map<String, Object> o2) {
			int ret = 0;
			for(int i = 0, len = arrCols.length; i < len; i ++) {
				if(!arrColsOrder[i]) {
					ret = getCompare(o1,o2,arrCols[i]);
				}else {
					ret = getCompare(o2,o1,arrCols[i]);
				}
				if(ret != 0) {
					break;
				}
			}
			return ret;
		}
		
		private int getCompare(Map<String, Object> o1, Map<String, Object> o2,String col) {
			Object oo1 = o1.get(col);
			Object oo2 = o2.get(col);
			if(oo1 instanceof Integer && oo1 instanceof Integer) {
				Integer no1 = (Integer)oo1;
				Integer no2 = (Integer)oo2;
				return no1.compareTo(no2);
			}
			if(oo1 instanceof Long && oo1 instanceof Long) {
				Long no1 = (Long)oo1;
				Long no2 = (Long)oo2;
				return no1.compareTo(no2);
			}
			if(oo1 instanceof Double && oo1 instanceof Double) {
				Double no1 = (Double)oo1;
				Double no2 = (Double)oo2;
				return no1.compareTo(no2);
			}
			if(oo1 instanceof Float && oo1 instanceof Float) {
				Float no1 = (Float)oo1;
				Float no2 = (Float)oo2;
				return no1.compareTo(no2);
			}
			if(oo1 instanceof String && oo1 instanceof String) {
				String no1 = (String)oo1;
				String no2 = (String)oo2;
				return no1.compareTo(no2);
			}
			return 0;
		}
	}
	
	public static void orderListMap(List<Map<String, Object>> list,String[] cols,boolean[] colsDesc){
		Collections.sort(list, new ListMapComparator(cols,colsDesc));
	}
	
	/**
	 * 下划线转驼峰
	 * @param index
	 * @return
	 */
	public static String line2Hump(String index){
		int count = 0;
		char[] ochars = index.toCharArray();
		int len = ochars.length;
		char[] nchars = new char[len];
		char c;
		for(int i = 0; i < len; i++){
			c = ochars[i];
			if(c == '_'){
				count ++;
				c = (char)(ochars[++i] - 32);
				nchars[i - count] = c;
				continue;
			}else{
				nchars[i - count] = c;
			}
		}
		return new String(nchars, 0, len - count);
	}
}
