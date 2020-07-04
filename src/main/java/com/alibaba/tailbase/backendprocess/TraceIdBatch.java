package com.alibaba.tailbase.backendprocess;

import com.alibaba.tailbase.Constants;

import java.util.*;

public class TraceIdBatch {
    // 对应 Client 的批次

    private int batchPos = 0;
    private int processCount = 0;
    private boolean isLast = false;

    // Todo 不用每批都存，可以只放在对应线程中
    private Map<String,List<String>> client1FullAbandonTrace = new HashMap<>();
    private Map<String,List<String>> client2FullAbandonTrace = new HashMap<>();

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


    public Map<String, List<String>> getClient1FullAbandonTrace() {
        return client1FullAbandonTrace;
    }

    public void setClient1FullAbandonTrace(Map<String, List<String>> client1FullAbandonTrace) {
        this.client1FullAbandonTrace = client1FullAbandonTrace;
    }

    public Map<String, List<String>> getClient2FullAbandonTrace() {
        return client2FullAbandonTrace;
    }

    public void setClient2FullAbandonTrace(Map<String, List<String>> client2FullAbandonTrace) {
        this.client2FullAbandonTrace = client2FullAbandonTrace;
    }
}
