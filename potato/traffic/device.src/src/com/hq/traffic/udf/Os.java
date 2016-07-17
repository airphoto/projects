package com.hq.traffic.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class Os extends UDF {

	private Text k = new Text();
	
	public Text evaluate(final Text s) {
		String tmp = s.toString().toLowerCase();
		if(tmp != null){
			if(tmp.contains("windows")){
				k.set("Windows");
			}else if(tmp.contains("mac")){
				k.set("Mac");
			}else if(tmp.contains("linux")){
				k.set("Linux");
			}else if(tmp.contains("ios")){
				k.set("IOS");
			}else if(tmp.contains("android")){
				k.set("Android");
			}else{
				k.set("Other");
			}
		}else{
			k.set("Other");
		}
			
		return k;
	}
	
}
