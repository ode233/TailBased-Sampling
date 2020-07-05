package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import com.alibaba.tailbase.backendprocess.TraceIdBatch;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.tailbase.backendprocess.BackendProcessData.getStartTime;


public class ClientProcessData implements Runnable {

//    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());


    public static int THREAD_COUNT = 2;

    private static final int ALL_CLIENT_CACHE_NUM = 40;

    private static int CLIENT_CACHE_NUM;

    // 第一个线程永久保存尾三批，最后一个线程永久保存首二批，其它线程永久保存首二批、尾三批，至少要有三批的自由空间，计算方法：首部永久保存数+max(尾部永久保存数,最少自由空间)
    private static final int CLIENT_CACHE_NUM_MIN = 5;

    private static final List<ClientProcess> threadList = new ArrayList<>();

    private static URL url = null;



    public static void start() {
        new Thread(new ClientProcessData(), "ClientProcessDataThread").start();
    }

    public static  void init() {
        CLIENT_CACHE_NUM = ALL_CLIENT_CACHE_NUM / THREAD_COUNT;
        if(CLIENT_CACHE_NUM < CLIENT_CACHE_NUM_MIN){
            CLIENT_CACHE_NUM = CLIENT_CACHE_NUM_MIN;
        }
        for (int i = 0; i < THREAD_COUNT; i++) {
            ClientProcess clientProcess = new ClientProcess(i);
            threadList.add(clientProcess);
            for (int j = 0; j < CLIENT_CACHE_NUM; j++){
                clientProcess.BATCH_TRACE_LIST.put(j,new HashMap<>(Constants.BATCH_SIZE));
            }
        }
    }

    @Override
    public void run() {
        String path = getPath();
        // process data on client, not server
        if (StringUtils.isEmpty(path)) {
//            LOGGER.warn("path is empty");
            return;
        }
        try {
            url = new URL(path);
//            LOGGER.info("data path:" + path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            long totalSize = httpConnection.getContentLengthLong();
//            LOGGER.info("file totalSize: "+ totalSize);
            long slice = totalSize/ THREAD_COUNT;
            for (int i = 0; i < THREAD_COUNT; i++) {
                threadList.get(i).from = i*slice;
                if(i == THREAD_COUNT - 1){
                    threadList.get(i).to = totalSize - 1;
                }
                else {
                    threadList.get(i).to = (i+1)*slice - 1;
                }
                new Thread(threadList.get(i)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *  call backend controller to update wrong tradeId list.
     * @param badTraceIdSet batchPos批次的所有wrongTraceId
     * @param batchPos
     */
    private static void updateWrongTraceId(Set<String> badTraceIdSet, int batchPos, int threadID, Boolean isFinish) {
        String json = JSON.toJSONString(badTraceIdSet);
        // 无论List是否为空都必须发起一次Request，因为Backend需要统计操作次数
        //if (badTraceIdSet.size() > 0) {
            try {
//                LOGGER.info("updateBadTraceId, batchPos:" + batchPos);
                RequestBody body = new FormBody.Builder()
                        .add("traceIdListJson", json)
                        .add("batchPos", batchPos + "")
                        .add("threadID", threadID + "")
                        .add("isFinish", isFinish + "").build();
                Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceId").post(body).build();
                Response response = Utils.callHttp(request);
                response.close();
            } catch (Exception e) {
//                LOGGER.warn("fail to updateBadTraceId, json:" + json + ", batch:" + batchPos);
            }
        //}
    }

    // notify backend process when client process has finished.
    private static void callFinish(int threadID, String abandonFirstString,
                                   String abandonLastString, String port) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("threadID", threadID + "")
                    .add("abandonFirstString",abandonFirstString)
                    .add("abandonLastString",abandonLastString)
                    .add("port",port)
                    .build();
            Request request = new Request.Builder().url("http://localhost:8002/finish").post(body).build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
//            LOGGER.warn("fail to callFinish");
        }
    }


    public static String getWrongTrace(String wrongTraceIdList, int batchPos, int threadID) {
        HashSet<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<HashSet<String>>(){});
        Map<String,List<Map<Long,String>>> wrongTraceMap = new HashMap<>();
        int previous = batchPos - 1;
        int next = batchPos + 1;
        getWrongTraceWithBatch(previous, traceIdList, wrongTraceMap, threadID);
        getWrongTraceWithBatch(batchPos, traceIdList,  wrongTraceMap, threadID);
        getWrongTraceWithBatch(next, traceIdList, wrongTraceMap, threadID);
        // to clear spans, don't block client process thread. TODO to use lock/notify
        if(previous > 1 || (previous >= 0 && threadID == 0)){
            ClientProcess clientProcess = threadList.get(threadID);
            clientProcess.BATCH_TRACE_LIST.remove(previous);
            clientProcess.BATCH_TRACE_LIST.put(clientProcess.needAddBatchPos,new HashMap<>(Constants.BATCH_SIZE));
            clientProcess.needAddBatchPos ++;
            if (clientProcess.traceMap == null){
                clientProcess.lock.lock();
                try{
                    clientProcess.condition.signal();
                } finally {
                    clientProcess.lock.unlock();
                }
            }
        }
//        LOGGER.info("getWrongTrace, batchPos:" + batchPos + " thread: "+ threadID);
        for(List<Map<Long,String>> list : wrongTraceMap.values()){
            list.sort(Comparator.comparing(o -> o.entrySet().iterator().next().getKey()));
        }
        return JSON.toJSONString(wrongTraceMap);
    }

    private static void getWrongTraceWithBatch(int batchPos,  HashSet<String> traceIdList,
                                               Map<String,List<Map<Long,String>>> wrongTraceMap,
                                               int threadID) {
        // 不只是开头或结束第一批，第二批也要重新考虑，因为第一批可能不足2w条，同时还要考虑拼接的可不可能是属于其中的一个错误Trace
        // donot lock traceMap,  traceMap may be clear anytime.
        Map<String, List<String>> traceMap = threadList.get(threadID).BATCH_TRACE_LIST.get(batchPos);
        // traceMap为空时就不需要再去找了
        if(traceMap == null || traceMap.size() == 0){
            return;
        }
        for (String traceId : traceIdList) {
            List<String> spanList = traceMap.get(traceId);
            if (spanList != null) {
                // one trace may cross to batch (e.g batch size 20000, span1 in line 19999, span2 in line 20001)
                List<Map<Long, String>> existSpanList = wrongTraceMap.computeIfAbsent(traceId, k -> new ArrayList<>());
                for(String s : spanList){
                    HashMap<Long, String> map = new HashMap<>();
                    map.put(getStartTime(s),s);
                    existSpanList.add(map);
                }
            }
        }
    }

    /**
     *  Client 获取对应的数据源URL
     *  (8000 <- trace1.data)
     *  (8001 <= trace2.data)
     */
    private String getPath(){
        String port = System.getProperty("server.port", "8080");
        if (Constants.CLIENT_PROCESS_PORT1.equals(port)) {
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
        } else if (Constants.CLIENT_PROCESS_PORT2.equals(port)){
            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
        } else {
            return null;
        }
    }

    public static class ClientProcess implements Runnable {

        private long from;
        private long to;
        private final int threadID;
        private String abandonFirstString = "";
        private String abandonLastString = "";
        private final Map<Integer,Map<String,List<String>>> BATCH_TRACE_LIST = new HashMap<>(CLIENT_CACHE_NUM);

        private final Lock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();

        private final Set<String> badTraceIdList = new HashSet<>(1000);

        private int batchPos = 0;

        private int needAddBatchPos = CLIENT_CACHE_NUM;

        private Map<String, List<String>> traceMap;

        public ClientProcess(int threadID) {
            this.threadID = threadID;
        }

        @Override
        public void run() {
            try {
                HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
                httpConnection.setRequestProperty("Range", "bytes=" + from + "-" + to);
                InputStream input = httpConnection.getInputStream();
                BufferedReader bf = new BufferedReader(new InputStreamReader(input));
                long count = 0;
                traceMap = BATCH_TRACE_LIST.get(batchPos);
                if(threadID !=0){
                    abandonFirstString = bf.readLine();
                }
                String nextLine = bf.readLine();

                while (nextLine != null) {
                    String line = nextLine;
                    nextLine = bf.readLine();
                    if(nextLine==null && threadID != THREAD_COUNT - 1){
                        abandonLastString = line;
                        break;
                    }

                    count++;
                    String[] cols = line.split("\\|");
                    if (cols.length > 1) {
                        String traceId = cols[0];
                        traceMap.computeIfAbsent(traceId, k -> new ArrayList<>()).add(line);
                        if (cols.length > 8) {
                            String tags = cols[8];
                            if (tags != null) {
                                if (tags.contains("error=1")) {
                                    badTraceIdList.add(traceId);
                                } else if (tags.contains("http.status_code=") && !tags.contains("http.status_code=200")) {
                                    badTraceIdList.add(traceId);
                                }
                            }
                        }
                    }
                    if (count % Constants.BATCH_SIZE == 0) {
                        updateWrongTraceId(badTraceIdList, batchPos, threadID, false);
                        badTraceIdList.clear();
                        batchPos++;
                        traceMap = BATCH_TRACE_LIST.get(batchPos);
                        if (traceMap == null){
                            lock.lock();
                            try{
                                condition.await();
                                traceMap = BATCH_TRACE_LIST.get(batchPos);
                            }finally {
                                lock.unlock();
                            }
                        }
                    }
                }
                updateWrongTraceId(badTraceIdList, batchPos, threadID, true);
                bf.close();
                input.close();
                callFinish(threadID,abandonFirstString,abandonLastString,System.getProperty("server.port", "8080"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static String getAbandonWrongTrace(String abandonTracesJson, String allClientConcatTraceJson) {
        Map<Integer,Map<Integer,TraceIdBatch>> abandonTraces
                = JSON.parseObject(abandonTracesJson, new TypeReference<Map<Integer,Map<Integer,TraceIdBatch>>>(){});
        Map<Integer,Map<String,List<String>>> allClientConcatTrace =
                JSON.parseObject(allClientConcatTraceJson, new TypeReference<Map<Integer,Map<String,List<String>>>>(){});
        Map<String,List<Map<Long,String>>> wrongTraceMap = new HashMap<>();

        // 先补全缺失的trace
        for(int i = 0; i< THREAD_COUNT; i++){
            if (i != THREAD_COUNT -1){
                TraceIdBatch traceIdBatch = abandonTraces.get(i).get(2);
                Map<String,List<String>> clientConcatTrace = allClientConcatTrace.get(i);
//                LOGGER.info("isLast:" + traceIdBatch.isLast() +
//                        "getBatchPos:" + traceIdBatch.getBatchPos() + "THREAD_COUNT:" + i);
//                LOGGER.info(String.valueOf(clientConcatTrace));
                for(Map.Entry<String,List<String>> entry: clientConcatTrace.entrySet()){
                    threadList.get(i).BATCH_TRACE_LIST
                            .get(traceIdBatch.getBatchPos())
                            .computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
                }
            }
        }
        for(int i = 0; i< THREAD_COUNT; i++){
            // 处理开头第一批
            if (i !=0){
                TraceIdBatch traceIdBatch = abandonTraces.get(i).get(0);
                getWrongTraceWithBatch(abandonTraces.get(i-1).get(2).getBatchPos() - 1, traceIdBatch.getTraceIdList(), wrongTraceMap, i-1);
                getWrongTraceWithBatch(abandonTraces.get(i-1).get(2).getBatchPos(), traceIdBatch.getTraceIdList(), wrongTraceMap, i-1);
                getWrongTraceWithBatch(0, traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(1, traceIdBatch.getTraceIdList(), wrongTraceMap, i);
            }
            // 处理末尾第一批
            if (i != THREAD_COUNT -1){
                TraceIdBatch traceIdBatch = abandonTraces.get(i).get(2);
                getWrongTraceWithBatch(traceIdBatch.getBatchPos()-1, traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(traceIdBatch.getBatchPos(), traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(0, traceIdBatch.getTraceIdList(), wrongTraceMap, i+1);
            }
            // 处理末尾第二批
            if (i != THREAD_COUNT -1){
                TraceIdBatch traceIdBatch = abandonTraces.get(i).get(1);
                getWrongTraceWithBatch(traceIdBatch.getBatchPos()-1, traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(traceIdBatch.getBatchPos(), traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(traceIdBatch.getBatchPos()+1, traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(0, traceIdBatch.getTraceIdList(), wrongTraceMap, i+1);
            }
        }
//        LOGGER.info("getAbandonWrongTrace");
        for(List<Map<Long,String>> list : wrongTraceMap.values()){
            list.sort(Comparator.comparing(o -> o.entrySet().iterator().next().getKey()));
        }
        return JSON.toJSONString(wrongTraceMap);
    }

}
