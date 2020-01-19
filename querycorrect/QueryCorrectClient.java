package com.echeng.dps.tools.gmclient;

import com.alibaba.fastjson.JSON;
import com.echeng.dps.config.ESGlobalConfig;
import com.echeng.dps.log.ESLog;
import com.echeng.dps.util.HttpUtil;
import com.echeng.dps.util.TimeCalUtil;

import java.util.*;

public class QueryCorrectClient {
    private static QueryCorrectClient instance = new QueryCorrectClient();

    private static String environment = ESGlobalConfig.getInstance().getItem("environment").toLowerCase();
    private static String httpworkName = ESGlobalConfig.getInstance().getItem("query_correct_http_svr_" + environment);

    public static QueryCorrectClient getInstance() {
        return instance;
    }

    @SuppressWarnings("unchecked")
    public String QueryCorrect(String query) {
        TimeCalUtil tc = new TimeCalUtil();
        tc.start();

        String c = "query_correct";
        String m = "";
        Map<String, Object> p = new HashMap<String, Object>();
        p.put("query", query);

        Map<String, Object> req = new HashMap<String, Object>();
        req.put("header", new HashMap<Object,Object>());
        Map<String, Object> request = new HashMap<String, Object>();
        request.put("c", c);
        request.put("m", m);
        request.put("p", p);
        req.put("request", request);
        String requestStr = JSON.toJSONString(req).replaceAll("\t", "");

        Map<String, Object> response = null;
        String correctResult = query;
        int retry = 0;
        while (retry++ < 3) {
            try {
                response = (Map<String, Object>) JSON.parseObject(HttpUtil.sendHttpPost(httpworkName, requestStr));
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        tc.end();
        ESLog.getInstance().info(QueryCorrectClient.class,
                String.format("call query_correct work, query:%s, times:%d, cost:%dms", query, retry, tc.getDuration()));
        if (response == null) {
            return correctResult;
        }

        //坑爹的response
        if (!response.containsKey("response")) {
            return correctResult;
        }

        Map<String, Object> tmpResponse = (Map<String, Object>) response.get("response");

        if (!tmpResponse.containsKey("results")) {
            return correctResult;
        }

        if (!(tmpResponse.get("results") instanceof Map)) {
            return correctResult;
        }

        Map<String, Object> results = (Map<String, Object>) tmpResponse.get("results");
        if (!results.containsKey("corrected_query")) {
            return correctResult;
        }

        correctResult = (String) results.get("corrected_query");

        return correctResult;
    }

    public static void main(String[] args) {
        System.out.println("hello world");
        QueryCorrectClient qcc = new QueryCorrectClient();
        String query = "andrid开法工成师";
        String result = qcc.QueryCorrect(query);
        System.out.println(query + " --> " + result);
    }

}
