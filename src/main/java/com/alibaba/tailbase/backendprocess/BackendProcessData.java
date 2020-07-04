package com.alibaba.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.tailbase.Constants.*;
import static com.alibaba.tailbase.clientprocess.ClientProcessData.THREAD_COUNT;

public class BackendProcessData implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendController.class.getName());

    private static volatile Integer BACKEND_FINISH_THREAD_COUNT = 0;

    private static List<BackendProcess> BackendTHREADLIST = new ArrayList<>();

    private static Map<String, String> TRACE_CHECKSUM_MAP= new ConcurrentHashMap<>();

    private static String[] ports = new String[]{CLIENT_PROCESS_PORT1, CLIENT_PROCESS_PORT2};

    private static final int ALL_SERVER_CACHE_NUM = 360;

    private static int SERVER_CACHE_NUM;

    // 第一个线程永久保存尾两批，最后一个线程永久保存首一批，其它线程永久保存首一批、尾两批,至少要有1批的空间，计算方法：首部永久保存数+max(尾部永久保存数,最少自由空间)
    private static final int SERVER_CACHE_NUM_MIN = 3;

    public static void init() {
        for (int i = 0; i < THREAD_COUNT; i++) {
            BackendProcess backendProcess = new BackendProcess(i);
            BackendTHREADLIST.add(backendProcess);
            SERVER_CACHE_NUM = ALL_SERVER_CACHE_NUM / THREAD_COUNT;
            if(SERVER_CACHE_NUM < SERVER_CACHE_NUM_MIN){
                SERVER_CACHE_NUM = SERVER_CACHE_NUM_MIN;
            }
        }
    }

    public static void start() {
        new Thread(new BackendProcessData(), "BackendProcessThread").start();
    }

    @Override
    public void run() {
        for(int i=0; i<THREAD_COUNT; i++){
            new Thread(BackendTHREADLIST.get(i)).start();
        }
        while (true){
            if (BACKEND_FINISH_THREAD_COUNT == THREAD_COUNT){
                LOGGER.info("begin handelAbandonWrongTrace");
                handelAbandonWrongTrace();
                if (sendCheckSum()) {
                    break;
                }
            }
        }
    }

    /**
     * call client process, to get all spans of wrong traces.
     * @param traceIdList
     * @param port
     * @param batchPos
     * @return
     */
    private static Map<String,List<Map<Long,String>>>  getWrongTrace(@RequestParam String traceIdList, String port, int batchPos, int threadID) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("traceIdList", traceIdList).add("batchPos", batchPos + "")
                    .add("threadID", threadID + "").build();
            String url = String.format("http://localhost:%s/getWrongTrace", port);
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            Map<String,List<Map<Long,String>>> resultMap = JSON.parseObject(response.body().string(),
                    new TypeReference<Map<String, List<Map<Long,String>>>>() {});
            response.close();
            return resultMap;
        } catch (Exception e) {
            LOGGER.warn("fail to getWrongTrace,batchPos:" + batchPos, e);
        }
        return null;
    }


    private boolean sendCheckSum() {
        try {
            String result = JSON.toJSONString(TRACE_CHECKSUM_MAP);
            RequestBody body = new FormBody.Builder()
                    .add("result", result).build();
            String url = String.format("http://localhost:%s/api/finished", CommonController.getDataSourcePort());
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            if (response.isSuccessful()) {
                response.close();
                LOGGER.warn("suc to sendCheckSum");
                return true;
            }
            LOGGER.warn("fail to sendCheckSum:" + response.message());
            response.close();
            return false;
        } catch (Exception e) {
            LOGGER.warn("fail to call finish", e);
        }
        return false;
    }

    public static long getStartTime(String span) {
        if (span != null) {
            String[] cols = span.split("\\|");
            if (cols.length > 8) {
                return Utils.toLong(cols[1], -1);
            }
        }
        return -1;
    }


    /**
     * trace batch will be finished, when client process has finished.(FINISH_PROCESS_COUNT == PROCESS_COUNT)
     * @return
     */
    public static boolean isFinished(int threadID) {
        if (BackendTHREADLIST.get(threadID).FINISH_CLIENT_COUNT < Constants.CLIENT_COUNT){
            return false;
        }
        Map<Integer, TraceIdBatch> traceIdBatches = BackendTHREADLIST.get(threadID).traceIdBatches;
        // 第一个线程永久保存尾两批，最后一个线程永久保存首一批，其它线程永久保存首一批、尾两批
        if(threadID == 0){
            return traceIdBatches.size() <= 2;
        }
        else if (threadID == THREAD_COUNT - 1){
            return traceIdBatches.size() <= 1;
        }
        else {
            return traceIdBatches.size() <= 3;
        }
    }

    /**
     * get finished bath when current and next batch has all finished
     * @return
     */
    public static TraceIdBatch getFinishedBatch(int threadId) {
        int current = BackendTHREADLIST.get(threadId).CURRENT_BATCH;
        int next = current + 1;
        Map<Integer, TraceIdBatch> traceIdBatches = BackendTHREADLIST.get(threadId).traceIdBatches;
        TraceIdBatch nextBatch = traceIdBatches.get(next);
        TraceIdBatch currentBatch = traceIdBatches.get(current);

        if(currentBatch == null || nextBatch == null){
            if (currentBatch != null && currentBatch.isLast() && threadId == THREAD_COUNT - 1) {
                traceIdBatches.remove(current);
                BackendTHREADLIST.get(threadId).CURRENT_BATCH = next;
                return currentBatch;
            }
            else {
                return null;
            }
        }

//        LOGGER.info("getFinishedBatch " + nextBatch.getBatchPos() + "count:"+ nextBatch.getProcessCount());
//        LOGGER.info("getFinishedBatch " + currentBatch.getBatchPos() + "count:"+ currentBatch.getProcessCount());

        // when client process is finished, or then next trace batch is finished. to get checksum for wrong traces.
        boolean cond1 = BackendTHREADLIST.get(threadId).FINISH_CLIENT_COUNT >= CLIENT_COUNT;
        boolean cond2 = currentBatch.getProcessCount() >= CLIENT_COUNT && nextBatch.getProcessCount() >= CLIENT_COUNT;
        if (cond1 || cond2) {
            if((current == 0) && threadId != 0){
                BackendTHREADLIST.get(threadId).CURRENT_BATCH = next;
                return null;
            }
            if(nextBatch.isLast() && threadId != THREAD_COUNT - 1){
                BackendTHREADLIST.get(threadId).CURRENT_BATCH = next;
                return null;
            }
            else {
                traceIdBatches.remove(current);
                BackendTHREADLIST.get(threadId).CURRENT_BATCH = next;
                return currentBatch;
            }
        }
        return null;
    }

    public synchronized static String setWrongTraceId(@RequestParam String traceIdListJson, @RequestParam int batchPos,
                                         @RequestParam int threadID, @RequestParam boolean isFinish) {
        List<String> traceIdList = JSON.parseObject(traceIdListJson, new TypeReference<List<String>>() {
        });
        LOGGER.info(String.format("setWrongTraceId had called, batchPos:%d", batchPos));
        Map<Integer, TraceIdBatch> traceIdBatches = BackendTHREADLIST.get(threadID).traceIdBatches;
        TraceIdBatch traceIdBatch = traceIdBatches.get(batchPos);

        // TODO to use lock/notify
        while (traceIdBatches.size() >= SERVER_CACHE_NUM){
//            LOGGER.info(String.valueOf(traceIdBatches.size()));
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 不能有 traceIdList.size() > 0
        if (traceIdList != null) {
            if (traceIdBatch == null){
                traceIdBatch = new TraceIdBatch();
                traceIdBatches.put(batchPos, traceIdBatch);
            }
            traceIdBatch.setBatchPos(batchPos);
            traceIdBatch.getTraceIdList().addAll(traceIdList);
            traceIdBatch.setProcessCount(traceIdBatch.getProcessCount() + 1);
            if(isFinish){
                traceIdBatch.setLast(true);
            }
            LOGGER.info("setWrongTraceId " + batchPos + traceIdBatch.isLast());
        }
        return "suc";
    }

    public synchronized static String finish(int threadID, String abandonFirstString, String abandonLastString, String port) {
        BackendTHREADLIST.get(threadID).FINISH_CLIENT_COUNT++;
        if (Constants.CLIENT_PROCESS_PORT1.equals(port)) {
            BackendTHREADLIST.get(threadID).client1AbandonFirstString = abandonFirstString;
            BackendTHREADLIST.get(threadID).client1AbandonLastString = abandonLastString;
        }
        else if (Constants.CLIENT_PROCESS_PORT2.equals(port)) {
            BackendTHREADLIST.get(threadID).client2AbandonFirstString = abandonFirstString;
            BackendTHREADLIST.get(threadID).client2AbandonLastString = abandonLastString;
        }
        LOGGER.warn("receive call 'finish', count:" + BackendTHREADLIST.get(threadID).FINISH_CLIENT_COUNT);
        return "suc";
    }

    private static String mergeSort(List<Map<Long,String>> list1, List<Map<Long,String>> list2){
        StringBuilder s = new StringBuilder();
        int n1 = list1.size();
        int n2 = list2.size();
        int i1 = 0, i2 = 0;
        while(i1 < n1 && i2 < n2){
            Map.Entry<Long,String> entry1 = list1.get(i1).entrySet().iterator().next();
            Map.Entry<Long,String> entry2 = list2.get(i2).entrySet().iterator().next();
            if(entry1.getKey() <= entry2.getKey()){
                s.append(entry1.getValue());
                s.append("\n");
                i1++;
            }
            else{
                s.append(entry2.getValue());
                s.append("\n");
                i2++;
            }
        }
        while (i1 < n1){
            Map.Entry<Long,String> entry1 = list1.get(i1).entrySet().iterator().next();
            s.append(entry1.getValue());
            s.append("\n");
            i1++;
        }
        while (i2 < n2){
            Map.Entry<Long,String> entry2 = list2.get(i2).entrySet().iterator().next();
            s.append(entry2.getValue());
            s.append("\n");
            i2++;
        }
//        LOGGER.info("list1:\n"+list1);
//        LOGGER.info("list2:\n"+list2);
//        LOGGER.info("wrong trace:\n"+s);
        return Utils.MD5(s.toString());
    }
    

    public static class BackendProcess implements Runnable {
        private int threadID;
        private Map<Integer, TraceIdBatch> traceIdBatches = new HashMap<>();
        private volatile Integer FINISH_CLIENT_COUNT = 0;
        private volatile Integer CURRENT_BATCH = 0;

        private String client1AbandonFirstString = "";
        private String client1AbandonLastString = "";

        private String client2AbandonFirstString = "";
        private String client2AbandonLastString = "";


        public BackendProcess(int threadID){
            this.threadID = threadID;
        }

        @Override
        public void run() {
            TraceIdBatch traceIdBatch = null;
            while (true) {
                try {
                    traceIdBatch = getFinishedBatch(threadID);

                    if (traceIdBatch == null) {
//                        LOGGER.info("traceIdBatch is null");
                        // send checksum when client process has all finished.
                        if (isFinished(threadID)) {
                            BACKEND_FINISH_THREAD_COUNT++;
                            break;
                        }
                        continue;
                    }
                    int batchPos = traceIdBatch.getBatchPos();
                    Map<String, List<Map<Long,String>>> processMap1 = getWrongTrace(JSON.toJSONString(traceIdBatch.getTraceIdList()), ports[0], batchPos, threadID);
                    Map<String, List<Map<Long,String>>> processMap2 = getWrongTrace(JSON.toJSONString(traceIdBatch.getTraceIdList()), ports[1], batchPos, threadID);
                    getWrongTraceMD5(processMap1, processMap2);
                    LOGGER.info("getWrong:" + batchPos);

                } catch (Exception e) {
                    // record batchPos when an exception  occurs.
                    int batchPos = 0;
                    if (traceIdBatch != null) {
                        batchPos = traceIdBatch.getBatchPos();
                    }
                    LOGGER.warn(String.format("fail to getWrongTrace, batchPos:%d", batchPos), e);
                } finally {
                    if (traceIdBatch == null) {
                        try {
                            Thread.sleep(100);
                        } catch (Throwable e) {
                            // quiet
                        }
                    }
                }
            }
        }
    }

    private static void getWrongTraceMD5(Map<String, List<Map<Long,String>>> processMap1, Map<String, List<Map<Long,String>>> processMap2){
        if(processMap1 != null){
            for(Map.Entry<String, List<Map<Long,String>>> entry : processMap1.entrySet()){
                List<Map<Long,String>> list1 = entry.getValue();
                List<Map<Long,String>> list2 = new ArrayList<>();
                String traceId = entry.getKey();
                if(processMap2 != null){
                    list2 = processMap2.computeIfAbsent(traceId, k -> new ArrayList<>());
                    processMap2.remove(traceId);
                }
                TRACE_CHECKSUM_MAP.put(traceId, mergeSort(list1, list2));
            }
        }
        if(processMap2 != null){
            for(Map.Entry<String, List<Map<Long,String>>> entry : processMap2.entrySet()){
                List<Map<Long,String>> list2 = entry.getValue();
                List<Map<Long,String>> list1 = new ArrayList<>();
                String traceId = entry.getKey();
                if(processMap1 != null){
                    list1 = processMap1.computeIfAbsent(traceId, k -> new ArrayList<>());
                    processMap1.remove(traceId);
                }
                TRACE_CHECKSUM_MAP.put(traceId, mergeSort(list1, list2));
            }
        }
    }

    private static void handelAbandonWrongTrace(){
        // <进程号,<批次位置,批次数据>>  0 第一批，2最后一批，1最后第二批
        Map<Integer,Map<Integer,TraceIdBatch>> abandonWrongTraces = new HashMap<>();

        // <进程号,<,>>
        Map<Integer,Map<String,List<String>>> allClient1ConcatTrace = new HashMap<>();
        Map<Integer,Map<String,List<String>>> allClient2ConcatTrace = new HashMap<>();

        for(int i = 0; i< THREAD_COUNT; i++){
            Map<Integer,TraceIdBatch> batchWrongTrace = new HashMap<>();
            for(Map.Entry<Integer, TraceIdBatch> entry: BackendTHREADLIST.get(i).traceIdBatches.entrySet()){
                int key;
                if (entry.getValue().getBatchPos() == 0){
                    key = 0;
                }
                else if (entry.getValue().isLast()){
                    key = 2;
                }
                else {
                    key = 1;
                }
                batchWrongTrace.put(key,entry.getValue());
//                LOGGER.info("handelAbandonWrongTrace thread: "+ i + " "+ entry.getValue().isFirst() + " " + entry.getValue().getTraceIdList());
            }
            if(i != THREAD_COUNT - 1){
                // 虑正好读一行的情况，也就是说是两个完整的trace
                allClient1ConcatTrace.put(i, concatLastTrace(BackendTHREADLIST.get(i).client1AbandonLastString,
                        BackendTHREADLIST.get(i + 1).client1AbandonFirstString,batchWrongTrace.get(2).getTraceIdList()));
                allClient2ConcatTrace.put(i, concatLastTrace(BackendTHREADLIST.get(i).client2AbandonLastString,
                        BackendTHREADLIST.get(i + 1).client2AbandonFirstString,batchWrongTrace.get(2).getTraceIdList()));

            }
            abandonWrongTraces.put(i,batchWrongTrace);
        }
        Map<String, List<Map<Long,String>>> processMap1 = getAbandonWrongTrace(JSON.toJSONString(abandonWrongTraces), ports[0], JSON.toJSONString(allClient1ConcatTrace));
        Map<String, List<Map<Long,String>>> processMap2 = getAbandonWrongTrace(JSON.toJSONString(abandonWrongTraces), ports[1], JSON.toJSONString(allClient2ConcatTrace));
        getWrongTraceMD5(processMap1, processMap2);
        LOGGER.info("finish handelAbandonWrongTrace");
    }

    private static Map<String, List<String>> concatLastTrace(String nowBatchAbandonLastString, String nextBatchAbandonFirstString, HashSet<String>  traceIdList){
        String s = nowBatchAbandonLastString + nextBatchAbandonFirstString;
        String[] cols = s.split("\\|");
        Map<String, List<String>> map = new HashMap<>();
        if (cols.length < 10){
            if (cols.length > 1) {
                String traceId = cols[0];
                // 添加缺失的trace
                map.computeIfAbsent(traceId, k -> new ArrayList<>()).add(s);
                if (cols.length > 8) {
                    String tags = cols[8];
                    if (tags != null) {
                        if (tags.contains("error=1")) {
                            traceIdList.add(traceId);
                        } else if (tags.contains("http.status_code=") && !tags.contains("http.status_code=200")) {
                            traceIdList.add(traceId);
                        }
                    }
                }
            }
        }
        else {
            String[] nowBatchColsLast = nowBatchAbandonLastString.split("\\|");
            String[] nextBatchColsFirst = nextBatchAbandonFirstString.split("\\|");
            // 添加缺失的trace
            if (nowBatchColsLast.length > 1) {
                String traceId = nowBatchColsLast[0];
                map.computeIfAbsent(traceId, k -> new ArrayList<>()).add(nowBatchAbandonLastString);
                if (nowBatchColsLast.length > 8) {
                    String tags = nowBatchColsLast[8];
                    if (tags != null) {
                        if (tags.contains("error=1")) {
                            traceIdList.add(traceId);
                        } else if (tags.contains("http.status_code=") && !tags.contains("http.status_code=200")) {
                            traceIdList.add(traceId);
                        }
                    }
                }
            }
            if (nextBatchColsFirst.length > 1) {
                String traceId = nextBatchColsFirst[0];
                map.computeIfAbsent(traceId, k -> new ArrayList<>()).add(nextBatchAbandonFirstString);
                if (nextBatchColsFirst.length > 8) {
                    String tags = nextBatchColsFirst[8];
                    if (tags != null) {
                        if (tags.contains("error=1")) {
                            traceIdList.add(traceId);
                        } else if (tags.contains("http.status_code=") && !tags.contains("http.status_code=200")) {
                            traceIdList.add(traceId);
                        }
                    }
                }
            }
        }
        return map;
    }

    private static Map<String,List<Map<Long,String>>> getAbandonWrongTrace(@RequestParam String abandonTraces, String port, String allClientConcatTrace) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("abandonTraces", abandonTraces)
                    .add("allClientConcatTrace", allClientConcatTrace).build();
            String url = String.format("http://localhost:%s/getAbandonWrongTrace", port);
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            Map<String,List<Map<Long,String>>> resultMap = JSON.parseObject(response.body().string(),
                    new TypeReference<Map<String, List<Map<Long,String>>>>() {});
            response.close();
            return resultMap;
        } catch (Exception e) {
            LOGGER.warn("fail to getAbandonTrace", e);
        }
        return null;
    }
}
