package com.alibaba.tailbase.backendprocess;

import com.alibaba.tailbase.Constants;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class TraceIdBatch {
    // 对应 Client 的批次

    private int batchPos = 0;
    private int processCount = 0;
    private int processId = 0;
    private boolean isFirst = false;
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

    public int getProcessId() {
        return processId;
    }

    public void setProcessId(int processId) {
        this.processId = processId;
    }

    public boolean isFirst() {
        return isFirst;
    }

    public void setFirst(boolean first) {
        isFirst = first;
    }

    public boolean isLast() {
        return isLast;
    }

    public void setLast(boolean last) {
        isLast = last;
    }
}
