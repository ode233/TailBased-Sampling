package com.alibaba.tailbase.backendprocess;

import com.alibaba.tailbase.Constants;

import java.util.*;

public class TraceIdBatch {
    // 对应 Client 的批次

    private int batchPos = 0;
    private int processCount = 0;
    private boolean isLast = false;

    private HashSet<String> traceIdList = new HashSet<>(Constants.BATCH_SIZE / 10);

    public int getBatchPos() {
        return batchPos;
    }

    public void setBatchPos(int batchPos) {
        this.batchPos = batchPos;
    }

    public int getProcessCount() {
        return processCount;
    }

    public void setProcessCount(int processCount) {
        this.processCount = processCount;
    }

    public HashSet<String> getTraceIdList() {
        return traceIdList;
    }

    public boolean isLast() {
        return isLast;
    }

    public void setLast(boolean last) {
        isLast = last;
    }

}
