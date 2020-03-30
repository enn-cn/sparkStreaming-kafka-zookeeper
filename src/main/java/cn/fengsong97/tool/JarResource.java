package cn.fengsong97.tool;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class JarResource {
	public static InputStream getResource(String filename) throws IOException {
		// 返回读取指定资源的输入流
		InputStream is = JarResource.class.getClassLoader().getResourceAsStream(filename);
		// InputStream is=当前类.class.getResourceAsStream("XX.config");
		System.out.println("stream :"+ is);
		return is;
	}
	
	
	public static String getResourcePath(String filename){
		URL resource = JarResource.class.getClassLoader().getResource(filename);
		if(resource == null){
			return "";
		}
		String path = resource.getPath();
		return path;
	}
}
