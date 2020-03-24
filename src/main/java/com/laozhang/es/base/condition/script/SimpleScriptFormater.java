package com.laozhang.es.base.condition.script;

public class SimpleScriptFormater{
	public final static String COMMON_CASEWHEN_SCRIPT_FORMAT = "if(%s %s '%s'){return %s;}else{return %s}";
	
	public final static String COMMON_COLNAME_SCRIPT_FORMAT = "doc.%s.value";
	
	public final static String PARAMS_PREFIX = "params.";
}
