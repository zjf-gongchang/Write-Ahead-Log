package com.gongchang.wal.core.base;

import java.nio.file.Path;
import java.nio.file.Paths;

public class WalConstant {

    private WalConstant(){}

    /**
     * 预写日志父路径
     */
    public static final Path WAL_PAREND_PATH = Paths.get(System.getProperty("user.dir") ,"wal");

}
