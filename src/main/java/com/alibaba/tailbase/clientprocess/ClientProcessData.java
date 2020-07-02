package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import com.alibaba.tailbase.backendprocess.BackendProcessData;
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
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.tailbase.Constants.PROCESS_COUNT;
import static com.alibaba.tailbase.backendprocess.BackendProcessData.getStartTime;


public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());


    private static int BATCH_COUNT = 20;

    public static int PROCESS_COUNT = 20;

    private static List<UnitDownloader> process = new ArrayList<>();

    private static URL url = null;


    public static void start() {
        new Thread(new ClientProcessData(), "ClientProcessDataThread").start();
    }

    public static  void init() {
        for (int i = 0; i < PROCESS_COUNT; i++) {
            UnitDownloader unitDownloader = new UnitDownloader(i);
            process.add(unitDownloader);
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
            long slice = totalSize/PROCESS_COUNT;
            for (int i = 0; i < PROCESS_COUNT; i++) {
                process.get(i).from = Math.min(i*slice, totalSize-1);
                process.get(i).to = Math.min((i+1)*slice, totalSize-1);
                new Thread(process.get(i)).start();
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
    private static void updateWrongTraceId(Set<String> badTraceIdSet, int batchPos, int processId) {
        String json = JSON.toJSONString(badTraceIdSet);
        // 无论List是否为空都必须发起一次Request，因为Backend需要统计操作次数
        //if (badTraceIdSet.size() > 0) {
            try {
                LOGGER.info("updateBadTraceId, batchPos:" + batchPos);
                RequestBody body = new FormBody.Builder()
                        .add("traceIdListJson", json)
                        .add("batchPos", batchPos + "")
                        .add("processId", processId + "").build();
                Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceId").post(body).build();
                Response response = Utils.callHttp(request);
                response.close();
            } catch (Exception e) {
                LOGGER.warn("fail to updateBadTraceId, json:" + json + ", batch:" + batchPos);
            }
        //}
    }

    // notify backend process when client process has finished.
    private static void callFinish(int processId) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("processId", processId + "").build();
            Request request = new Request.Builder().url("http://localhost:8002/finish").post(body).build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            LOGGER.warn("fail to callFinish");
        }
    }


    public static String getWrongTrace(String wrongTraceIdList, int batchPos, int processId) {
        HashSet<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<HashSet<String>>(){});
        Map<String,List<Map<Long,String>>> wrongTraceMap = new HashMap<>();
        int pos = batchPos % BATCH_COUNT;
        int previous = pos - 1;
        if (previous == -1) {
            previous = BATCH_COUNT -1;
        }
        int next = pos + 1;
        if (next == BATCH_COUNT) {
            next = 0;
        }
        getWrongTraceWithBatch(previous, traceIdList, wrongTraceMap, processId);
        getWrongTraceWithBatch(pos, traceIdList,  wrongTraceMap, processId);
        getWrongTraceWithBatch(next, traceIdList, wrongTraceMap, processId);
        // to clear spans, don't block client process thread. TODO to use lock/notify
        process.get(processId).BATCH_TRACE_LIST.get(previous).clear();
        LOGGER.info("getWrongTrace, batchPos:" + batchPos);
        for(List<Map<Long,String>> list : wrongTraceMap.values()){
            list.sort(Comparator.comparing(o -> o.entrySet().iterator().next().getKey()));
        }
        return JSON.toJSONString(wrongTraceMap);
    }

    private static void getWrongTraceWithBatch(int batchPos,  HashSet<String> traceIdList,
                                               Map<String,List<Map<Long,String>>> wrongTraceMap,
                                               int processId) {
        // donot lock traceMap,  traceMap may be clear anytime.
        Map<String, List<String>> traceMap = process.get(processId).BATCH_TRACE_LIST.get(batchPos);
        // traceMap为空时就不需要再去找了
        if(traceMap.size() == 0){
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
        private int processId;
        private String abandonFirstString = "";
        private String abandonLastString = "";
        private List<Map<String,List<String>>> abandonFirstBatchList = new ArrayList<>();
        private List<Map<String,List<String>>> abandonLastBatchList = new ArrayList<>();
        private List<Map<String,List<String>>> BATCH_TRACE_LIST = new ArrayList<>();

        public UnitDownloader(int processId) {
            this.processId = processId;
            for (int i = 0; i < BATCH_COUNT; i++) {
                BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
            }
            for (int i = 0; i < 2; i++) {
                abandonFirstBatchList.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
                abandonLastBatchList.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
            }
        }

        @Override
        public void run() {
            try {
                HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
                httpConnection.setRequestProperty("Range", "bytes=" + from + "-" + to);
                InputStream input = httpConnection.getInputStream();
                BufferedReader bf = new BufferedReader(new InputStreamReader(input));
                long count = 0;
                int pos = 0;
                Set<String> badTraceIdList = new HashSet<>(1000);
                Map<String, List<String>> traceMap = BATCH_TRACE_LIST.get(pos);
                if(processId !=0){
                    abandonFirstString = bf.readLine();
                }
                String nextLine = bf.readLine();

                while (true) {
                    String line = nextLine;
                    nextLine = bf.readLine();
                    if(nextLine==null){
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
                        // batchPos begin from 0, so need to minus 1
                        int batchPos = (int) count / Constants.BATCH_SIZE - 1;
                        if((batchPos == 0 || batchPos == 1)){
                            if(processId != 0){
                                abandonFirstBatchList.get(batchPos).putAll(traceMap);
                            }
                        }
                        else {
                            if(processId != PROCESS_COUNT - 1){
                                abandonLastBatchList.remove(0);
                                abandonLastBatchList.get(1).putAll(traceMap);
                            }
                        }
                        updateWrongTraceId(badTraceIdList, batchPos, processId);
                        badTraceIdList.clear();

                        pos++;
                        // loop cycle
                        if (pos >= BATCH_COUNT) {
                            pos = 0;
                        }
                        traceMap = BATCH_TRACE_LIST.get(pos);
                        // donot produce data, wait backend to consume data
                        // TODO to use lock/notify
                        if (traceMap.size() > 0) {
                            while (true) {
                                Thread.sleep(10);
                                if (traceMap.size() == 0) {
                                    break;
                                }
                            }
                        }
                    }
                }
                updateWrongTraceId(badTraceIdList, (int) (count / Constants.BATCH_SIZE - 1), processId);
                bf.close();
                input.close();
                callFinish(processId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static String getAbandonWrongTrace(String wrongTraceIdList, int batchPos, int processId) {
        HashSet<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<HashSet<String>>(){});
        Map<String,List<Map<Long,String>>> wrongTraceMap = new HashMap<>();
        int pos = batchPos % BATCH_COUNT;
        int previous = pos - 1;
        if (previous == -1) {
            previous = BATCH_COUNT -1;
        }
        int next = pos + 1;
        if (next == BATCH_COUNT) {
            next = 0;
        }
        getWrongTraceWithBatch(previous, traceIdList, wrongTraceMap, processId);
        getWrongTraceWithBatch(pos, traceIdList,  wrongTraceMap, processId);
        getWrongTraceWithBatch(next, traceIdList, wrongTraceMap, processId);
        // to clear spans, don't block client process thread. TODO to use lock/notify
        process.get(processId).BATCH_TRACE_LIST.get(previous).clear();
        LOGGER.info("getWrongTrace, batchPos:" + batchPos);
        for(List<Map<Long,String>> list : wrongTraceMap.values()){
            list.sort(Comparator.comparing(o -> o.entrySet().iterator().next().getKey()));
        }
        return JSON.toJSONString(wrongTraceMap);
    }

    private static void getAbandonWrongTraceWithBatch(int batchPos,  HashSet<String> traceIdList,
                                               Map<String,List<Map<Long,String>>> wrongTraceMap,
                                               int processId) {
        // donot lock traceMap,  traceMap may be clear anytime.
        Map<String, List<String>> traceMap = process.get(processId).BATCH_TRACE_LIST.get(batchPos);
        // traceMap为空时就不需要再去找了
        if(traceMap.size() == 0){
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

}
