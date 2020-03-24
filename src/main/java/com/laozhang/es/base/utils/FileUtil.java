package com.laozhang.es.base.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileUtil {

	public static List<File> getFiles(String path){
		System.out.println(System.getProperty("user.dir"));
		File file = new File(path);
		File[] tempList = file.listFiles(new FilenameFilter(){
			@Override
			public boolean accept(File dir, String name) {
				if(name.endsWith(".txt")){
					return true;
				}else{
					return false;
				}
			}
			
		});

		return Arrays.asList(tempList);
	}
	
	public static Map<String,String> readToString(File file) throws FileNotFoundException, IOException {
		try(BufferedReader br = new BufferedReader(new FileReader(file))) {
			Map<String,String> data = new HashMap<>();
		    StringBuilder sb = new StringBuilder();
		    String line = br.readLine();

		    while (line != null) {
		        sb.append(line);
		        sb.append(System.lineSeparator());
		        line = br.readLine();
		    }
		    data.put("indexName", file.getName().replace(".txt", ""));
		    data.put("indexData",sb.toString());
		    return data;
		}
    }  
}