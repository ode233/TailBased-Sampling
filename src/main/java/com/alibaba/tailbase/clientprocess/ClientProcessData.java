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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.*;

import static com.alibaba.tailbase.backendprocess.BackendProcessData.getStartTime;


public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());


    public static int THREAD_COUNT = 2;

    private static List<UnitDownloader> threadList = new ArrayList<>();

    private static URL url = null;


    public static void start() {
        new Thread(new ClientProcessData(), "ClientProcessDataThread").start();
    }

    public static  void init() {
        for (int i = 0; i < THREAD_COUNT; i++) {
            UnitDownloader unitDownloader = new UnitDownloader(i);
            threadList.add(unitDownloader);
        }
    }

    @Override
    public void run() {
        String path = getPath();
        // process data on client, not server
        if (StringUtils.isEmpty(path)) {
            LOGGER.warn("path is empty");
            return;
        }
        try {
            url = new URL(path);
            LOGGER.info("data path:" + path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            long totalSize = httpConnection.getContentLengthLong();
            LOGGER.info("file totalSize: "+ totalSize);
            long slice = totalSize/ THREAD_COUNT;
            for (int i = 0; i < THREAD_COUNT; i++) {
                threadList.get(i).from = Math.min(i*slice, totalSize-1);
                threadList.get(i).to = Math.min((i+1)*slice, totalSize-1);
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
                LOGGER.info("updateBadTraceId, batchPos:" + batchPos);
                RequestBody body = new FormBody.Builder()
                        .add("traceIdListJson", json)
                        .add("batchPos", batchPos + "")
                        .add("threadID", threadID + "")
                        .add("isFinish", isFinish + "").build();
                Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceId").post(body).build();
                Response response = Utils.callHttp(request);
                response.close();
            } catch (Exception e) {
                LOGGER.warn("fail to updateBadTraceId, json:" + json + ", batch:" + batchPos);
            }
        //}
    }

    // notify backend process when client process has finished.
    private static void callFinish(int threadID, String abandonFirstString,
                                   String abandonLastString) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("threadID", threadID + "")
                    .add("abandonFirstString",abandonFirstString)
                    .add("abandonLastString",abandonLastString).build();
            Request request = new Request.Builder().url("http://localhost:8002/finish").post(body).build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            LOGGER.warn("fail to callFinish");
        }
    }


    public static String getWrongTrace(String wrongTraceIdList, int batchPos, int threadID) {
        HashSet<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<HashSet<String>>(){});
        Map<String,List<Map<Long,String>>> wrongTraceMap = new HashMap<>();
        int pos = batchPos;
        int previous = pos - 1;
        int next = pos + 1;
        getWrongTraceWithBatch(previous, traceIdList, wrongTraceMap, threadID);
        getWrongTraceWithBatch(pos, traceIdList,  wrongTraceMap, threadID);
        getWrongTraceWithBatch(next, traceIdList, wrongTraceMap, threadID);
        // to clear spans, don't block client process thread. TODO to use lock/notify
        if(previous > 1){
            threadList.get(threadID).BATCH_TRACE_LIST.remove(previous);
        }
        LOGGER.info("getWrongTrace, batchPos:" + batchPos);
        for(List<Map<Long,String>> list : wrongTraceMap.values()){
            list.sort(Comparator.comparing(o -> o.entrySet().iterator().next().getKey()));
        }
        return JSON.toJSONString(wrongTraceMap);
    }

    private static void getWrongTraceWithBatch(int batchPos,  HashSet<String> traceIdList,
                                               Map<String,List<Map<Long,String>>> wrongTraceMap,
                                               int threadID) {
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

    /**
     *  Client的数据源，用于本地测试。将trace1.data和trace2.data置于文件服务器根目录下。
     */
    private String getPathNative(){
        String port = System.getProperty("server.port", "8080");
        if ("8000".equals(port)) {
            return "http://localhost:8888/trace1.data";
        } else if ("8001".equals(port)){
            return "http://localhost:8888/trace2.data";
        } else {
            return null;
        }
    }

    public static class UnitDownloader implements Runnable {

        private long from;
        private long to;
        private int threadID;
        private String abandonFirstString = "";
        private String abandonLastString = "";
        private Map<Integer,Map<String,List<String>>> BATCH_TRACE_LIST = new HashMap<>();

        public UnitDownloader(int threadID) {
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
                int batchPos = 0;
                Set<String> badTraceIdList = new HashSet<>(1000);
                Map<String, List<String>> traceMap = new HashMap<>();
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
                        BATCH_TRACE_LIST.put(batchPos, traceMap);
                        // TODO 需要判断是不是不是最后一批
                        updateWrongTraceId(badTraceIdList, batchPos, threadID, false);
                        badTraceIdList.clear();
                        batchPos++;

                    }
                }
                updateWrongTraceId(badTraceIdList, batchPos, threadID, true);
                bf.close();
                input.close();
                callFinish(threadID,abandonFirstString,abandonLastString);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static String getAbandonWrongTrace(String abandonTraces) {
        Map<Integer,Map<Boolean,TraceIdBatch>> abandonTraceMap
                = JSON.parseObject(abandonTraces, new TypeReference<Map<Integer,Map<Boolean,TraceIdBatch>>>(){});
        Map<String,List<Map<Long,String>>> wrongTraceMap = new HashMap<>();
        for(int i = 0; i< THREAD_COUNT; i++){
            // 处理末尾
            if (i != THREAD_COUNT -1){
                TraceIdBatch traceIdBatch = abandonTraceMap.get(i).get(false);
                getWrongTraceWithBatch(traceIdBatch.getBatchPos()-1, traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(traceIdBatch.getBatchPos(), traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(0, traceIdBatch.getTraceIdList(), wrongTraceMap, i+1);
            }
            // 处理开头
            if (i !=0){
                TraceIdBatch traceIdBatch = abandonTraceMap.get(i).get(true);
                getWrongTraceWithBatch(0, traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(1, traceIdBatch.getTraceIdList(), wrongTraceMap, i);
                getWrongTraceWithBatch(abandonTraceMap.get(i-1).get(false).getBatchPos(), traceIdBatch.getTraceIdList(), wrongTraceMap, i-1);
            }
        }
        LOGGER.info("getAbandonWrongTrace");
        for(List<Map<Long,String>> list : wrongTraceMap.values()){
            list.sort(Comparator.comparing(o -> o.entrySet().iterator().next().getKey()));
        }
        return JSON.toJSONString(wrongTraceMap);
    }

}
