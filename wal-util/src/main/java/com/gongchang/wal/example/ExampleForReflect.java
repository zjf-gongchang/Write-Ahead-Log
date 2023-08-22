package com.gongchang.wal.example;

import com.alibaba.fastjson.JSONObject;
import com.gongchang.wal.WalRecoverUtils;
import com.gongchang.wal.WalSinkUtils;
import com.gongchang.wal.core.base.WalEntry;

public class ExampleForReflect {

	public static void main(String[] args) {
		System.out.println("请求提交开始》》》》》》》》》》》》》》》》");
		for(int i=0; i<10; i++){
			JSONObject data = new JSONObject();
			data.put("business_field", "business_field_value");
			WalEntry walEntry = new WalEntry(new ReflectTestRetryDo(), data);
			WalSinkUtils.write(walEntry);
		}
		
		System.out.println("数据恢复开始》》》》》》》》》》》》》》》》");
		WalRecoverUtils.recover();
	}
}

