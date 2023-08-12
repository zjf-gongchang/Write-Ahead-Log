package com.gongchang.wal.core.read;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * 读预写日志
 */
public class ReadAheadLogImpl implements ReadAheadLog {

    private static final Logger logger = LoggerFactory.getLogger(ReadAheadLogImpl.class);


    private Path walPath;

    public ReadAheadLogImpl(Path walPath) {
        this.walPath = walPath;
    }


    @Override
    public Iterator<String> readLog() throws IOException {
        Iterator iterator = new Iterator() {
            private List<Path> pathList = new LinkedList<>();
            Iterator<String> curLines;

            {
                try {
                    Files.list(walPath)
                            .filter(path -> path.getFileName().toString().indexOf("-")>0)
                            .forEach(path -> pathList.add(path));
                } catch (IOException e) {
                    logger.error("获取预写日志路径异常：", e);
                }
            }

            @Override
            public boolean hasNext() {
                if(curLines==null || !curLines.hasNext()){
                    while(pathList.size()>0){
                        Path nextPath = pathList.remove(0);
                        try {
							curLines = Files.lines(nextPath).iterator();
						} catch (IOException e) {
							logger.error("读取预写日志异常", e);
						}
                        if(curLines.hasNext()){
                            return true;
                        }
                    }
                    return false;
                }else{
                    return true;
                }
            }

            @Override
            public Object next() {
                return curLines.next();
            }

        };
        return iterator;
    }
}
