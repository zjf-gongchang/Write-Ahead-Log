package com.gongchang.wal.core.write;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;


/**
 * 请求预写日志Base类
 */
public abstract class SyncWriteAheadLog extends AbstractWriteAheadLog<String> {

    private static final Logger logger = LoggerFactory.getLogger(SyncWriteAheadLog.class);
    
    
    public SyncWriteAheadLog(String logName) {
        super(logName);
    }
    
	public SyncWriteAheadLog(String logName, Integer maxHisLogNum) {
		super(logName, maxHisLogNum);
	}


	@Override
    public synchronized void writeLog(String value) throws IOException {
        Path curWalPath = getCurWalPath();
        try(BufferedWriter bw = Files.newBufferedWriter(curWalPath, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
            bw.write(value);
            bw.newLine();
            bw.flush();

            checkCutLog(value.getBytes().length);
        }catch (IOException e) {
            logger.error("写入预写日志异常", e);
            throw e;
        }
    }

}
