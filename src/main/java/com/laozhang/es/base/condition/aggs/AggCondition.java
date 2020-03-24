package com.laozhang.es.base.condition.aggs;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import com.laozhang.es.base.condition.common.Arithmetic;

public abstract class AggCondition {
	
	protected AggregationBuilder cursor;
	
	public abstract String getName();
	
	public abstract Object getCondition();
	
	public AggregationBuilder getCursor(){
		return this.cursor;
	}
	
	protected static Script getScript(Arithmetic a,Object opNum){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("_pValue", opNum);
		StringBuilder sb = new StringBuilder();
		sb.append("_value").append(a.getName()).append("params._pValue");
		Script script = new Script(ScriptType.INLINE,Script.DEFAULT_SCRIPT_LANG,sb.toString(),params);
		return script;
	}
	
	protected static Script getScript(String scriptStr){
		return new Script(scriptStr);
	}

	protected static Script getScript(String scriptStr,Map<String,Object> params){
		return new Script(ScriptType.INLINE,Script.DEFAULT_SCRIPT_LANG,scriptStr,params);
	}
}
