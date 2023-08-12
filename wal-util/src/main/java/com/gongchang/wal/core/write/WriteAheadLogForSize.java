package com.gongchang.wal.core.write;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteAheadLogForSize extends WriteAheadLogBase {

    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogForSize.class);


    private static final Long DEFAULT_MAX_LOG_SIZE = 1*1024*1024*1024L;

    private static final Integer DEFAULT_MAX_HIS_LOG_NUM = 7;


    private Long logSize = 0L;

    private Long maxLogSize = DEFAULT_MAX_LOG_SIZE;

    private Integer maxHisLogNum = DEFAULT_MAX_HIS_LOG_NUM;


    public WriteAheadLogForSize(String logName) throws IOException {
        super(logName);
    }

    public WriteAheadLogForSize(String logName, Long maxLogSize) throws IOException {
        super(logName);
        this.maxLogSize = maxLogSize;
    }

    public WriteAheadLogForSize(String logName, Integer maxHisLogNum) throws IOException {
        super(logName);
        this.maxHisLogNum = maxHisLogNum;
    }

    public WriteAheadLogForSize(String logName, Long maxLogSize, Integer maxHisLogNum) throws IOException {
        super(logName);
        this.maxLogSize = maxLogSize;
        this.maxHisLogNum = maxHisLogNum;
    }

    @Override
    public Boolean whetherToCut(String value) {
        logSize+=value.getBytes().length;
        if(logSize>=maxLogSize){
            logger.info("当前日志达到最大日志大小");
            return true;
        }else{
            return false;
        }
    }

    @Override
    public List<String> getCleanLogName() {
        return  getDicOrderCleanLogName(maxHisLogNum);
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
