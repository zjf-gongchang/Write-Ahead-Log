package com.gongchang.wal.example;

import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSONObject;
import com.gongchang.wal.WalRecoverUtils;
import com.gongchang.wal.WalSinkUtils;
import com.gongchang.wal.core.base.WalEntry;

public class ExampleForReflect {

	public static void main(String[] args) {
		System.out.println("请求提交开始》》》》》》》》》》》》》》》》");
		long start = System.currentTimeMillis();
		for(int i=0; i<10000; i++){
			JSONObject data = new JSONObject();
			data.put("business_field", "business_field_value");
			WalEntry walEntry = new WalEntry(new ReflectTestRetryDo(), data);
			WalSinkUtils.write(walEntry);
		}
		long end = System.currentTimeMillis();
		System.err.println("10000请求提交耗时"+ (end-start));
		
		try {
			TimeUnit.SECONDS.sleep(10);
		} catch (InterruptedException e) {
		}
		System.out.println("数据恢复开始》》》》》》》》》》》》》》》》");
//		WalRecoverUtils.recover();
	}
}

