package com.alibaba.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.tailbase.Constants.*;
import static com.alibaba.tailbase.Constants.PROCESS_COUNT;

public class BackendProcessData implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendController.class.getName());

    private static volatile Integer FINISH_PROCESS_COUNT = 0;

    private static volatile Integer CURRENT_BATCH = 0;

    private static int BATCH_COUNT = 90;

    private static List<TraceIdBatch> TRACEID_BATCH_LIST= new ArrayList<>();

    private static Map<String, String> TRACE_CHECKSUM_MAP= new ConcurrentHashMap<>();

    public static  void init() {
        for (int i = 0; i < BATCH_COUNT; i++) {
            TRACEID_BATCH_LIST.add(new TraceIdBatch());
        }
    }

    public static void start() {
        new Thread(new BackendProcessData(), "BackendProcessThread").start();
    }

    @Override
    public void run() {
        TraceIdBatch traceIdBatch = null;
        String[] ports = new String[]{CLIENT_PROCESS_PORT1, CLIENT_PROCESS_PORT2};
        while (true) {
            try {
                traceIdBatch = getFinishedBatch();

                if (traceIdBatch == null) {
                    // send checksum when client process has all finished.
                    if (isFinished()) {
                        if (sendCheckSum()) {
                            break;
                        }
                    }
                    continue;
                }
                Map<String, Set<String>> map = new HashMap<>();
               // if (traceIdBatch.getTraceIdList().size() > 0) {
                    int batchPos = traceIdBatch.getBatchPos();
                    // to get all spans from remote
                    for (String port : ports) {
                        Map<String, List<String>> processMap =
                                getWrongTrace(JSON.toJSONString(traceIdBatch.getTraceIdList()), port, batchPos);
                        if (processMap != null) {
                            for (Map.Entry<String, List<String>> entry : processMap.entrySet()) {
                                String traceId = entry.getKey();
                                map.computeIfAbsent(traceId, k -> new HashSet<>()).addAll(entry.getValue());
                            }
                        }
                    }
                    LOGGER.info("getWrong:" + batchPos + ", traceIdsize:" + traceIdBatch.getTraceIdList().size() + ",result:" + map.size());
               // }

                for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
                    String traceId = entry.getKey();
                    Set<String> spanSet = entry.getValue();
                    // order span with startTime
                    String spans = spanSet.stream().sorted(
                            Comparator.comparing(BackendProcessData::getStartTime)).collect(Collectors.joining("\n"));
                    spans = spans + "\n";
                    // output all span to check
                   // LOGGER.info("traceId:" + traceId + ",value:\n" + spans);
                    TRACE_CHECKSUM_MAP.put(traceId, Utils.MD5(spans));
                }
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

    /**
     * call client process, to get all spans of wrong traces.
     * @param traceIdList
     * @param port
     * @param batchPos
     * @return
     */
    private Map<String,List<String>>  getWrongTrace(@RequestParam String traceIdList, String port, int batchPos) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("traceIdList", traceIdList).add("batchPos", batchPos + "").build();
            String url = String.format("http://localhost:%s/getWrongTrace", port);
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            Map<String,List<String>> resultMap = JSON.parseObject(response.body().string(),
                    new TypeReference<Map<String, List<String>>>() {});
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
    public static boolean isFinished() {
        for (int i = 0; i < BATCH_COUNT; i++) {
            TraceIdBatch currentBatch = TRACEID_BATCH_LIST.get(i);
            if (currentBatch.getBatchPos() != 0) {
                return false;
            }
        }
        return FINISH_PROCESS_COUNT >= Constants.PROCESS_COUNT;
    }

    /**
     * get finished bath when current and next batch has all finished
     * @return
     */
    public static TraceIdBatch getFinishedBatch() {
        int next = CURRENT_BATCH + 1;
        if (next >= BATCH_COUNT) {
            next = 0;
        }
        TraceIdBatch nextBatch = TRACEID_BATCH_LIST.get(next);
        TraceIdBatch currentBatch = TRACEID_BATCH_LIST.get(CURRENT_BATCH);

        // when client process is finished, or then next trace batch is finished. to get checksum for wrong traces.
        boolean cond1 = FINISH_PROCESS_COUNT >= PROCESS_COUNT && currentBatch.getBatchPos() > 0;
        boolean cond2 = currentBatch.getProcessCount() >= PROCESS_COUNT && nextBatch.getProcessCount() >= PROCESS_COUNT;
        if (cond1 || cond2) {
            TraceIdBatch newTraceIdBatch = new TraceIdBatch();
            TRACEID_BATCH_LIST.set(CURRENT_BATCH, newTraceIdBatch);
            CURRENT_BATCH = next;
            return currentBatch;
        }
        return null;
    }

    public static String setWrongTraceId(@RequestParam String traceIdListJson, @RequestParam int batchPos) {
        int pos = batchPos % BATCH_COUNT;
        List<String> traceIdList = JSON.parseObject(traceIdListJson, new TypeReference<List<String>>() {
        });
        LOGGER.info(String.format("setWrongTraceId had called, batchPos:%d", batchPos));
        TraceIdBatch traceIdBatch = TRACEID_BATCH_LIST.get(pos);

        // 不能有 traceIdList.size() > 0
        if (traceIdList != null) {
            traceIdBatch.setBatchPos(batchPos);
            traceIdBatch.setProcessCount(traceIdBatch.getProcessCount() + 1);
            traceIdBatch.getTraceIdList().addAll(traceIdList);
        }
        return "suc";
    }

    public static String finish() {
        FINISH_PROCESS_COUNT++;
        LOGGER.warn("receive call 'finish', count:" + FINISH_PROCESS_COUNT);
        return "suc";
    }
}
