package com.alibaba.tailbase.backendprocess;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BackendController {
    @RequestMapping("/setWrongTraceId")
    public String setWrongTraceId(@RequestParam String traceIdListJson, @RequestParam int batchPos,
                                  @RequestParam int threadID, @RequestParam boolean isFinish) {
        return BackendProcessData.setWrongTraceId(traceIdListJson, batchPos, threadID, isFinish);
    }

    @RequestMapping("/finish")
    public String finish(@RequestParam int threadID, @RequestParam String abandonFirstString,
                         @RequestParam String abandonLastString, @RequestParam String port) {
        return BackendProcessData.finish(threadID, abandonFirstString, abandonLastString, port);
    }
}
