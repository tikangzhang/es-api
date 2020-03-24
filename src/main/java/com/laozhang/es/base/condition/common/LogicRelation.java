package com.laozhang.es.base.condition.common;

public enum LogicRelation {
	Gt(">"),Gte(">="),Eq("=="),Ne("!="),Lt("<"),Lte("<=");
	
	private String name;
	
	LogicRelation(String name){
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
