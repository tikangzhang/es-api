package com.laozhang.es.base.exception;

public class NoRecordException extends EsException{
	
	private static final long serialVersionUID = 123499002L;

	public NoRecordException(){
		super("该记录不存在！");
	}
	
	public NoRecordException(String errMsg){
		super(errMsg);
	}
}
