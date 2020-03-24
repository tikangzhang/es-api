package com.laozhang.es.base.utils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

public class AnnotionUtil {
	public static List<Class<?>> getClasses(String packageName){
		//第一个class类的集合
		List<Class<?>> classes = new LinkedList<Class<?>>();
		//是否循环迭代
		boolean recursive = true;
		//获取包的名字并进行替换
		String packageDirName = packageName.replace('.' , '/');
		//定义一个枚举的集合并进行循环来处理这个目录下的things
		Enumeration<URL> dirs;
		try{
			dirs = Thread.currentThread().getContextClassLoader().getResources(packageDirName);
			//循环迭代下去
			while(dirs.hasMoreElements()){
				//获取下一个元素
				URL url = dirs.nextElement();
				//得到协议的名称
				String protocol = url.getProtocol();
				//如果是以文件的形式保存在服务器上
				if("file".equals(protocol)){
					//获取包的物理路径
					String filePath = URLDecoder.decode(url.getFile(),"UTF-8");
					//以文件的方式扫描整个包下的文件并添加到集合中
					findAndAddClassesInPackageByFile(packageName,filePath,recursive,classes);
				}
			}
		}catch(IOException e){
			e.printStackTrace();
		}
		return classes;
	}

	public static void findAndAddClassesInPackageByFile(String packageName,String packagePath,final boolean recursive,List<Class<?>>classes){
		//获取此包的目录建立一个File
		File dir = new File(packagePath);
		//如果不存在或者也不是目录就直接返回
		if(!dir.exists() || !dir.isDirectory()){
			return;
		}
		//如果存在就获取包下的所有文件包括目录
		File[] dirfiles = dir.listFiles(new FileFilter(){
		//自定义过滤规则如果可以循环(包含子目录)或则是以.class结尾的文件(编译好的java类文件)
				public boolean accept(File file){
					return(recursive && file.isDirectory()) || (file.getName().endsWith(".class"));
				}
			});
		//循环所有文件
		for(File file : dirfiles){
			//如果是目录则继续扫描
			if(file.isDirectory()){
				findAndAddClassesInPackageByFile(packageName + "." + file.getName(),
										file.getAbsolutePath(),
										recursive,
										classes);
			}
			else{
				//如果是java类文件去掉后面的.class只留下类名
				String className = file.getName().substring(0 ,file.getName().length()-6);
				try{
					//添加到集合中去
					classes.add(Class.forName(packageName + '.' + className));
				}catch(ClassNotFoundException e){
					e.printStackTrace();
				}
			}
		}
	}

}