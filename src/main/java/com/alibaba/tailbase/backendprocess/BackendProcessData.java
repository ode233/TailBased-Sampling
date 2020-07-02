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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.tailbase.Constants.*;
import static com.alibaba.tailbase.clientprocess.ClientProcessData.THREAD_COUNT;

public class BackendProcessData implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendController.class.getName());

    private static volatile Integer BACKEND_FINISH_THREAD_COUNT = 0;

    private static List<BackendProcess> BackendTHREADLIST = new ArrayList<>();

    private static Map<String, String> TRACE_CHECKSUM_MAP= new ConcurrentHashMap<>();

    private static String[] ports = new String[]{CLIENT_PROCESS_PORT1, CLIENT_PROCESS_PORT2};

    public static void init() {
        for (int i = 0; i < THREAD_COUNT; i++) {
            BackendProcess backendProcess = new BackendProcess(i);
            BackendTHREADLIST.add(backendProcess);
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
        for(Map.Entry<Integer, TraceIdBatch> entry :traceIdBatches.entrySet()){
            if (!entry.getValue().isFirst() && !entry.getValue().isLast()){
                LOGGER.info(String.valueOf(entry.getKey()));
                return false;
            }
        }
        return true;
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
                LOGGER.info("get last wrong\n" + currentBatch.getTraceIdList());
                return currentBatch;
            }
            else {
                return null;
            }
        }

        // when client process is finished, or then next trace batch is finished. to get checksum for wrong traces.
        boolean cond1 = BackendTHREADLIST.get(threadId).FINISH_CLIENT_COUNT >= CLIENT_COUNT;
        boolean cond2 = currentBatch.getProcessCount() >= CLIENT_COUNT && nextBatch.getProcessCount() >= CLIENT_COUNT;
        if (cond1 || cond2) {
            if(currentBatch.isFirst() && threadId != 0){
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

    public static String setWrongTraceId(@RequestParam String traceIdListJson, @RequestParam int batchPos,
                                         @RequestParam int threadID, @RequestParam boolean isFinish) {
        List<String> traceIdList = JSON.parseObject(traceIdListJson, new TypeReference<List<String>>() {
        });
        LOGGER.info(String.format("setWrongTraceId had called, batchPos:%d", batchPos));
        Map<Integer, TraceIdBatch> traceIdBatches = BackendTHREADLIST.get(threadID).traceIdBatches;
        TraceIdBatch traceIdBatch = traceIdBatches.get(batchPos);

        // 不能有 traceIdList.size() > 0
        if (traceIdList != null) {
            if (traceIdBatch == null){
                traceIdBatch = new TraceIdBatch();
                traceIdBatches.put(batchPos, traceIdBatch);
            }
            traceIdBatch.setBatchPos(batchPos);
            traceIdBatch.setProcessCount(traceIdBatch.getProcessCount() + 1);
            traceIdBatch.setThreadId(threadID);
            traceIdBatch.getTraceIdList().addAll(traceIdList);
            if(batchPos==0){
                traceIdBatch.setFirst(true);
            }
            if(isFinish){
                traceIdBatch.setLast(true);
            }
            LOGGER.info("setWrongTraceId " + batchPos +traceIdBatch.isFirst()+traceIdBatch.isLast());
        }
        return "suc";
    }

    public static String finish(int threadID, String abandonFirstString, String abandonLastString) {
        BackendTHREADLIST.get(threadID).FINISH_CLIENT_COUNT++;
        BackendTHREADLIST.get(threadID).abandonFirstString = abandonFirstString;
        BackendTHREADLIST.get(threadID).abandonLastString = abandonLastString;
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

        private String abandonFirstString = "";
        private String abandonLastString = "";


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
                        // send checksum when client process has all finished.
                        if (isFinished(threadID)) {
                            BACKEND_FINISH_THREAD_COUNT++;
                            break;
                        }
                        continue;
                    }
                    int batchPos = traceIdBatch.getBatchPos();
                    int threadID = traceIdBatch.getThreadId();
                    Map<String, List<Map<Long,String>>> processMap1 = getWrongTrace(JSON.toJSONString(traceIdBatch.getTraceIdList()), ports[0], batchPos, threadID);
                    Map<String, List<Map<Long,String>>> processMap2 = getWrongTrace(JSON.toJSONString(traceIdBatch.getTraceIdList()), ports[1], batchPos, threadID);
                    getWrongTraceMD5(processMap1, processMap2);
                    LOGGER.info("getWrong:" + batchPos + ", traceIdsize:" + traceIdBatch.getTraceIdList().size());

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
        Map<Integer,Map<Boolean,TraceIdBatch>> abandonTraces = new HashMap<>();
        for(int i = 0; i< THREAD_COUNT; i++){
            Map<Boolean,TraceIdBatch> map = new HashMap<>();
            for(Map.Entry<Integer, TraceIdBatch> entry: BackendTHREADLIST.get(i).traceIdBatches.entrySet()){
                if(i != THREAD_COUNT - 1 && !entry.getValue().isFirst()){
                    // TODO 考虑正好读一行的情况，也就是说是两个完整的trace
                    String s = BackendTHREADLIST.get(i).abandonLastString +
                            BackendTHREADLIST.get(i + 1).abandonFirstString;
                    String[] cols = s.split("\\|");
                    if (cols.length > 1) {
                        String traceId = cols[0];
                        if (cols.length > 8) {
                            String tags = cols[8];
                            if (tags != null) {
                                if (tags.contains("error=1")) {
                                    entry.getValue().getTraceIdList().add(traceId);
                                } else if (tags.contains("http.status_code=") && !tags.contains("http.status_code=200")) {
                                    entry.getValue().getTraceIdList().add(traceId);
                                }
                            }
                        }
                    }
                }
                map.put(entry.getValue().isFirst(),entry.getValue());
                LOGGER.info("handelAbandonWrongTrace thread: "+ i + " "+ entry.getValue().isFirst() + " " + entry.getValue().getTraceIdList());
            }
            abandonTraces.put(i,map);
        }
        Map<String, List<Map<Long,String>>> processMap1 = getAbandonWrongTrace(JSON.toJSONString(abandonTraces), ports[0]);
        Map<String, List<Map<Long,String>>> processMap2 = getAbandonWrongTrace(JSON.toJSONString(abandonTraces), ports[1]);
        getWrongTraceMD5(processMap1, processMap2);
        LOGGER.info("finish handelAbandonWrongTrace");
    }

    private static Map<String,List<Map<Long,String>>> getAbandonWrongTrace(@RequestParam String abandonTraces, String port) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("abandonTraces", abandonTraces).build();
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
