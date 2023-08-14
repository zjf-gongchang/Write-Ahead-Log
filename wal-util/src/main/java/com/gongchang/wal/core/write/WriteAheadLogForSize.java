package com.gongchang.wal.core.write;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;

public class WriteAheadLogForSize extends SyncWriteAheadLog {

    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogForSize.class);


    private AtomicLong logSize = new AtomicLong(0);

    private Long maxLogSize;


    public WriteAheadLogForSize(String logName) throws IOException {
        this(logName, WalConfig.DEFAULT_MAX_LOG_SIZE);
    }

    public WriteAheadLogForSize(String logName, Long maxLogSize) throws IOException {
        super(logName);
        this.maxLogSize = maxLogSize;
    }

    public WriteAheadLogForSize(String logName, Integer maxHisLogNum, Long maxLogSize) throws IOException {
        super(logName, maxHisLogNum);
        this.maxLogSize = maxLogSize;
    }

    
    @Override
    public Boolean whetherToCut(Integer logSize) {
        if(this.logSize.addAndGet(logSize)>=maxLogSize){
            logger.info("当前日志达到最大日志大小，切割日志");
            return true;
        }else{
            return false;
        }
    }

    @Override
    public List<String> getCleanLogName() {
        return getDicOrderCleanLogName();
    }

    @Override
    public String getNextLogName() {
        return String.valueOf(System.currentTimeMillis());
    }


    public static void main(String[] args) throws IOException {
        WriteAheadLogForSize rwalfs = new WriteAheadLogForSize("test");
        long start = System.currentTimeMillis();
        for(int i=1; i<=10001; i++){
            try {
                rwalfs.writeLog("hhhhhhhfdksajlkjekrwlqjk;rjelkwqoifuioc,msafjdsfdsahhhhhhhfdksajlkjekrwlqjk;rjelkwqoifuioc,msafjdsfdsahhhhhhhfdksajlkjekrwlqjk;rjelkwqoifuioc,msafjdsfdsahhhhhhhfdksajlkjekrwlqjk;rjelkwqoifuioc,msafjdsfdsahhhhhhhfdksajlkjekrwlqjk;rjelkwqoifuioc,msafjdsfdsahhhhhhhfdksajlkjekrwlqjk;rjelkwqoifuioc,msafjdsfdsahhhhhhhfdksajlkjekrwlqjk;rjelkwqoifuioc,msafjdsfdsahhhhhhhfdksajlkjekrwlqjk;rjelkwqoifuioc,msafjdsfdsahhhhhhhfdksajlkjekrwlqjk;rjelkwqoifuioc,msafjdsfdsa");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("耗时(毫秒)："+(end-start));

        int tmp = 0;
        Iterator<String> strings = null;

        /*try {
            strings = rwalfs.readLog();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        while(strings.hasNext()){
            tmp++;
            String next = strings.next();
            System.out.println(next);
        }
        System.out.println("读取结果"+tmp);
    }

}
