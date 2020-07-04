package com.alibaba.tailbase.clientprocess;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class ClientController {
    @RequestMapping("/getWrongTrace")
    public String getWrongTrace(@RequestParam String traceIdList, @RequestParam Integer batchPos,
                                @RequestParam Integer threadID) {
        return ClientProcessData.getWrongTrace(traceIdList, batchPos, threadID);
    }

    @RequestMapping("/getAbandonWrongTrace")
    public String getAbandonWrongTrace(@RequestParam String abandonTraces, @RequestParam String allClientConcatTrace) {
        return ClientProcessData.getAbandonWrongTrace(abandonTraces, allClientConcatTrace);
    }
}
