package com.gongchang.wal.core.base;

import com.alibaba.fastjson.JSONObject;

public interface WalDataCheck {

	Boolean check(JSONObject data);
	
}
