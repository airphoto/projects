package com.hq.hadoop.format.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Message {
	// "hqClientId","hqGroupId","hqAdId","hqCreativeId","hqRefer","hqTime","hqEvent","hqURL","hqPrice","hqSource","hquid",
	// "hqCity", "tagId", "channelId"
	private String hqClientId;
	private String hqCliendId;
	private String hqGroupId;
	private String hqAdId;
	private String hqCreativeId;
	private String hqRefer;
	private String hqTime;
	private String hqEvent;
	private String hqURL;
	private String hqPrice;
	private String hquid;
	private String hqCity;
	private String tagId;
	private String channelId;
	private String hqSource;
	private String cookie;
	private String id;
	private String did;
	private String impid;
	private String plmtid;
	
	public String getPlmtid() {
		return plmtid;
	}

	public void setPlmtid(String plmtid) {
		this.plmtid = plmtid;
	}

	public String getImpid() {
		return impid;
	}

	public void setImpid(String impid) {
		this.impid = impid;
	}

	public String getDid() {
		return did;
	}

	public void setDid(String did) {
		this.did = did;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCookie() {
		return cookie;
	}

	public void setCookie(String cookie) {
		this.cookie = cookie;
	}

	public String getHqCliendId() {
		return hqCliendId;
	}

	public void setHqCliendId(String hqCliendId) {
		this.hqCliendId = hqCliendId;
	}

	public String getHqSource() {
		return hqSource;
	}

	public void setHqSource(String hqSource) {
		this.hqSource = hqSource;
	}

	public String getHqClientId() {
		return hqClientId;
	}

	public void setHqClientId(String hqClientId) {
		this.hqClientId = hqClientId;
	}

	public String getHqGroupId() {
		return hqGroupId == null ? "0" : hqGroupId;
	}

	public void setHqGroupId(String hqGroupId) {
		this.hqGroupId = hqGroupId;
	}

	public String getHqAdId() {
		return hqAdId == null ? "0" : hqAdId;
	}

	public void setHqAdId(String hqAdId) {
		this.hqAdId = hqAdId;
	}

	public String getHqCreativeId() {
		return hqCreativeId == null ? "0" : hqCreativeId;
	}

	public void setHqCreativeId(String hqCreativeId) {
		this.hqCreativeId = hqCreativeId;
	}

	public String getHqRefer() {
		return hqRefer;
	}

	public void setHqRefer(String hqRefer) {
		this.hqRefer = hqRefer;
	}

	public String getHqTime() {
		return hqTime;
	}

	public void setHqTime(String hqTime) {
		this.hqTime = hqTime;
	}

	public String getHqEvent() {
		return hqEvent;
	}

	public void setHqEvent(String hqEvent) {
		this.hqEvent = hqEvent;
	}

	public String getHqURL() {
		return hqURL == null ? "-" : hqURL;
	}

	public void setHqURL(String hqURL) {
		this.hqURL = hqURL;
	}

	public String getHqPrice() {
		if (hqPrice == null || hqPrice == "") {
			return "0";
		}
		return hqPrice;
	}

	public void setHqPrice(String hqPrice) {
		this.hqPrice = hqPrice;
	}

	public String getHquid() {
		return hquid;
	}

	public void setHquid(String hquid) {
		this.hquid = hquid;
	}

	public String getHqCity() {
		return hqCity == null ? "-" : hqCity;
	}

	public void setHqCity(String hqCity) {
		this.hqCity = hqCity;
	}

	public String getTagId() {
		return tagId == null ? "-" : tagId;
	}

	public void setTagId(String tagId) {
		this.tagId = tagId;
	}

	public String getChannelId() {
		return channelId == null ? "-" : channelId;
	}

	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}
}
