package com.laozhang.es.tool;

import com.laozhang.es.base.mapping.IMappingCreator;
import com.laozhang.es.base.mapping.impl.CommonFileMappingCreator;
import com.laozhang.es.base.client.ClientDriver;

public class CommitSchema {
	public static void main(String[] args){
		if(args.length != 1){
			System.out.println("Usage: java -jar CommitSchema.jar path");
			System.exit(0);
		}
		IMappingCreator cmc = new CommonFileMappingCreator(args[0]);
		cmc.excute();
		ClientDriver.Destroy();
	}
}
