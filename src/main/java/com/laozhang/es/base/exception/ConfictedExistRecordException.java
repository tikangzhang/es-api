package com.laozhang.es.base.exception;

public class ConfictedExistRecordException extends EsException{
	
	private static final long serialVersionUID = 123499002L;

	public ConfictedExistRecordException(){
		super("重复冲突的记录存在！");
	}
	
	public ConfictedExistRecordException(String msg){
		super(msg);
	}
}
