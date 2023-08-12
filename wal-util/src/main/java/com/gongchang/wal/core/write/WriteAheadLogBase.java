package com.gongchang.wal.core.write;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConstant;


/**
 * 请求预写日志Base类
 */
public abstract class WriteAheadLogBase implements WriteAheadLog<String> {

    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogBase.class);


    private final ScheduledExecutorService logProcessSchedule = Executors.newSingleThreadScheduledExecutor();


    private Path walPath = null;

    private String logName;


    public WriteAheadLogBase(String logName) throws IOException {
        this.logName = logName;
        init();
    }


    private void init() throws IOException {
        // 初始化定时日志清理
        logProcessSchedule.scheduleWithFixedDelay(() -> cleanLog(),60, 60, TimeUnit.MINUTES);

        // 初始化预写日志根路径
        if(!Files.exists(WalConstant.WAL_PAREND_PATH)){
            try {
                Files.createDirectory(WalConstant.WAL_PAREND_PATH);
            } catch (IOException e) {
                logger.error("创建预写日志根目录异常", e);
                System.exit(1);
            }
        }

        // 初始化预写日志路径
        walPath = Paths.get(WalConstant.WAL_PAREND_PATH.toString(), logName);
        if(!Files.exists(walPath)){
            try {
                Files.createDirectory(walPath);
            } catch (IOException e) {
                logger.error("创建预写日志目录异常", e);
            }
        }

        // 初始化当前预写日志
        Path curWalPath = Paths.get(walPath.toString(), logName+".log");
        if(!Files.exists(curWalPath)){
            try {
                Files.createFile(curWalPath);
            } catch (IOException e) {
                logger.error("创建预写日志异常", e);
                throw e;
            }
        }else{
            Path tarWalPath = Paths.get(walPath.toString(), logName+"-"+System.currentTimeMillis()+".log");
            try {
                if(Files.size(curWalPath)>0){
                    Files.move(curWalPath,tarWalPath);
                    Files.createFile(curWalPath);
                }
            } catch (IOException e) {
                logger.error("移动预写日志异常", e);
                throw e;
            }
        }
    }

    @Override
    public synchronized void writeLog(String value) throws IOException {
        Path curWalPath = Paths.get(walPath.toString(), logName+".log");
        try(BufferedWriter bw = Files.newBufferedWriter(curWalPath, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
            bw.write(value);
            bw.newLine();
            bw.flush();

            if(whetherToCut(value)){
                Path tarWalPath = Paths.get(walPath.toString(), logName+"-"+getNextLogName()+".log");
                Files.move(curWalPath,tarWalPath);
                Files.createFile(curWalPath);
            }
        }catch (IOException e) {
            logger.error("写入预写日志异常", e);
            throw e;
        }
    }

    @Override
    public Boolean cleanLog(){
        try {
            List<String> cleanLogNameList = getCleanLogName();
            for (String cleanLogname: cleanLogNameList) {
                Files.delete(Paths.get(WalConstant.WAL_PAREND_PATH.toString(), cleanLogname));
            }
        }catch (Exception e){
            logger.error("日志清理异常：", e);
            return false;
        }
        return true;
    }

    public List<String> getDicOrderCleanLogName(Integer maxHisLogNum) {
        Path walPath = getWalPath();
        try {
            List<String> waitCleanLogNameList = Files.list(walPath).flatMap(path -> {
                List<String> tmpList = new ArrayList<>();
                String fileName = path.getFileName().toString();
                if (fileName.indexOf("-") > 0) {
                    tmpList.add(fileName);
                }
                return tmpList.stream();
            }).sorted().collect(Collectors.toList());

            List<String> cleanLogNameList;
            if(waitCleanLogNameList.size()<maxHisLogNum){
                cleanLogNameList=waitCleanLogNameList;
            }else{
                cleanLogNameList = new ArrayList<>();
                for(int i=0; i<waitCleanLogNameList.size()-maxHisLogNum; i++){
                    cleanLogNameList.add(waitCleanLogNameList.get(i));
                }
            }

            return  cleanLogNameList;
        } catch (IOException e) {
            logger.error("获取清理日志异常：", e);
        }
        return Collections.emptyList();
    }


    public abstract Boolean whetherToCut(String value);

    public abstract List<String> getCleanLogName();

    public abstract String getNextLogName();


    public Path getWalPath() {
        return walPath;
    }

    public void setWalPath(Path walPath) {
        this.walPath = walPath;
    }

}
