package com.laozhang.es.tool;

import com.laozhang.es.base.mapping.IMappingCreator;
import com.laozhang.es.base.mapping.impl.CommonFileMappingUpdator;
import com.laozhang.es.base.client.ClientDriver;

public class UpdateSchema {
	public static void main(String[] args){
		if(args.length != 1){
			System.out.println("Usage: java -jar CommitSchema.jar path");
			System.exit(0);
		}
		IMappingCreator cmc = new CommonFileMappingUpdator(args[0]);
		cmc.excute();
		ClientDriver.Destroy();
	}
}
