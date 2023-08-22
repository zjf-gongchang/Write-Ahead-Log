package com.gongchang.wal.core.base;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class Barrie implements StreamData {

	private Long barrieId;
	
	private Integer channel;
	
	
	public Barrie() {
		super();
		this.barrieId = System.currentTimeMillis();
		this.channel = 0;
	}

	
	@Override
	public String sdToMementoStr() {
		JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getStreamDataType());
        jsonObject.put("meta", this);
		return JSON.toJSONString(jsonObject);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Barrie sdFromMementoStr(String str) {
		JSONObject metaObject = JSON.parseObject(str).getJSONObject("meta");
		Barrie barrie = metaObject.toJavaObject(Barrie.class);
		return barrie;
	}


	public Long getBarrieId() {
		return barrieId;
	}

	public void setBarrieId(Long barrieId) {
		this.barrieId = barrieId;
	}

	public Integer getChannel() {
		return channel;
	}

	public void setChannel(Integer channel) {
		this.channel = channel;
	}


	@Override
	public StreamDataType getStreamDataType() {
		return StreamDataType.BARRIE;
	}
	
}
