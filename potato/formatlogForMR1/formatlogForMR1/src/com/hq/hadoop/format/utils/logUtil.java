package com.hq.hadoop.format.utils;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hq.hadoop.format.model.Message;

public class logUtil {

	public static String getKeyString(Message model) {

		String clientId = model.getHqClientId() != null ? model.getHqClientId() : model.getHqCliendId();
		String groupId = model.getHqGroupId();
		String adId = model.getHqAdId();
		String creativeId = model.getHqCreativeId();
		String refer = model.getHqRefer();
		String hqtime = model.getHqTime();
		String event = model.getHqEvent();
		String url = model.getHqURL();
		String hqprice = model.getHqPrice();
		String source = model.getHqSource();
		String uid = model.getHquid();
		String city = model.getHqCity();
		String tagid = model.getTagId();
		String channelid = model.getChannelId();
		String cookie = model.getCookie();
		String id = model.getId();
		String dest = "";
		String formatid = getUUID();
		String did = model.getDid();
		String impid = model.getImpid() == null ? id : model.getImpid();
		String plmtid = model.getPlmtid()!=null?model.getPlmtid():"";
		if (refer != null) {
			Pattern p = Pattern.compile("\\s*|\t|\r|\n");
			Matcher m = p.matcher(refer);
			dest = m.replaceAll("");
		}
		String result = clientId + "\t" + groupId + "\t" + adId + "\t" + creativeId + "\t" + dest + "\t" + hqtime + "\t"
				+ event + "\t" + url + "\t" + hqprice + "\t" + source + "\t" + uid + "\t" + city + "\t" + tagid + "\t"
				+ channelid + "\t" + cookie + "\t" + id + "\t" + formatid + "\t" + did + "\t" + impid + "\t" + plmtid;
		return result;
	}

	public static boolean filter(Message model) {
		List<String> platforms = getAllMessage("com.hq.hadoop.format.utils.platform");
		if (model.getHqSource() != null) {
			if (platforms.contains(model.getHqSource())) {
				return true;
			}
		}
		return false;
	}

	public static List<String> getAllMessage(String propertyName) {
		// 获得资源包
		ResourceBundle rb = ResourceBundle.getBundle(propertyName.trim());
		// 通过资源包拿到所有的key
		Enumeration<String> allKey = rb.getKeys();
		// 遍历key 得到 value
		List<String> valList = new ArrayList<String>();
		while (allKey.hasMoreElements()) {
			String key = allKey.nextElement();
			String value = (String) rb.getString(key);
			valList.add(value);
		}
		return valList;
	}

	public static String getUUID() {
		return UUID.randomUUID().toString();
	}
}
