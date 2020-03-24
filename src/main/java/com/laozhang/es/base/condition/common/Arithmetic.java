package com.laozhang.es.base.condition.common;

public enum Arithmetic {
	Add("+"),Subtract("-"),Multiply("*"),Divide("/");
	
	private String name;
	
	Arithmetic(String name){
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
