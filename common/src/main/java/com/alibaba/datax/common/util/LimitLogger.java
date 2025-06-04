package com.alibaba.datax.common.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jitongchen
 * @date 2023/9/7 9:47 AM
 */
public class LimitLogger {

    // use thread-safe map to avoid race conditions when logging from multiple threads
    private static final Map<String, Long> lastPrintTime = new ConcurrentHashMap<>();

    public static void limit(String name, long limit, LoggerFunction function) {
        if (StringUtils.isBlank(name)) {
            name = "__all__";
        }
        if (limit <= 0) {
            function.apply();
            return;
        }

        long now = System.currentTimeMillis();
        lastPrintTime.compute(name, (k, v) -> {
            if (v == null || now - v > limit) {
                function.apply();
                return now;
            }
            return v;
        });
    }
}
