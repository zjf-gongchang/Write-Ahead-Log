package com.gongchang.wal.core.base;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gongchang.wal.core.bus.RecoverContext;
import com.gongchang.wal.core.redo.RetryDo;
import com.gongchang.wal.core.redo.RetryDoRecover;


public class WalEntry implements StreamData {

    /**
     * 恢复数据的类的实例对象
     */
    private RetryDo retryDo;

    /**
     * 数据
     */
    private JSONObject data;

    /**
     * 创建时间
     */
    private Long barrieId;
    
    
    public WalEntry() {}

    public WalEntry(RetryDo retryDo, JSONObject data) {
        this(retryDo, data, System.currentTimeMillis());
    }

    private WalEntry(RetryDo retryDo, JSONObject data, Long barrieId) {
        this.retryDo = retryDo;
        this.data = data;
        this.barrieId = barrieId;
    }

    @Override
    public String sdToMementoStr() {
        WalEntryMeta walEntryMeta = new WalEntryMeta(
                retryDo.getClass().getCanonicalName(),
                retryDo.getRecoverBy().name(),
                data,
                barrieId);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", getStreamDataType());
        jsonObject.put("meta", walEntryMeta);
        return JSON.toJSONString(jsonObject);
    }

    @SuppressWarnings("unchecked")
	@Override
    public WalEntry sdFromMementoStr(String metaStr){
        JSONObject metaJsonObj = JSON.parseObject(metaStr).getJSONObject("meta");
        WalEntryMeta walEntryMeta = metaJsonObj.toJavaObject(WalEntryMeta.class);
        walEntryMeta.metaToWalEntry(this);
        return this;
    }
    
    @Override
	public StreamDataType getStreamDataType() {
		return StreamDataType.BUSINESS;
	}
    

    public static class WalEntryMeta{

        private String retryDoClassName;

        private String retryDoRecoverBy;

        private JSONObject dada;

        private Long barrieId;
        

        public WalEntryMeta(String retryDoClassName, String retryDoRecoverBy, JSONObject dada, Long barrieId) {
            this.retryDoClassName = retryDoClassName;
            this.retryDoRecoverBy = retryDoRecoverBy;
            this.dada = dada;
            this.barrieId = barrieId;
        }


        public WalEntry metaToWalEntry(){
            RetryDoRecover retryDoRecover = RecoverContext.getInstance().requestRetryDoRecover(retryDoRecoverBy);
            RetryDo retryDo = retryDoRecover.recover(retryDoClassName);
            WalEntry walEntry = new WalEntry(retryDo, dada, barrieId);
            return walEntry;
        }
        
        public void metaToWalEntry(WalEntry walEntry){
        	RetryDoRecover retryDoRecover = RecoverContext.getInstance().requestRetryDoRecover(retryDoRecoverBy);
        	RetryDo retryDo = retryDoRecover.recover(retryDoClassName);
        	walEntry.setRetryDo(retryDo);
        	walEntry.setData(dada);
        	walEntry.setBarrieId(barrieId);
        }


        public String getRetryDoClassName() {
            return retryDoClassName;
        }

        public void setRetryDoClassName(String retryDoClassName) {
            this.retryDoClassName = retryDoClassName;
        }

        public String getRetryDoRecoverBy() {
            return retryDoRecoverBy;
        }

        public void setRetryDoRecoverBy(String retryDoRecoverBy) {
            this.retryDoRecoverBy = retryDoRecoverBy;
        }

        public JSONObject getDada() {
            return dada;
        }

        public void setDada(JSONObject dada) {
            this.dada = dada;
        }

        public Long getBarrieId() {
            return barrieId;
        }

        public void setBarrieId(Long barrieId) {
            this.barrieId = barrieId;
        }

    }


    public RetryDo getRetryDo() {
        return retryDo;
    }

    public void setRetryDo(RetryDo retryDo) {
        this.retryDo = retryDo;
    }

    public JSONObject getData() {
        return data;
    }

    public void setData(JSONObject data) {
        this.data = data;
    }
    
	public Long getBarrieId() {
        return barrieId;
    }

    public void setBarrieId(Long barrieId) {
        this.barrieId = barrieId;
    }

}
