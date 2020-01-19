package com.echeng.dps.search.server.handle.impl;

import com.echeng.dps.batch.BatchMgr;
import com.echeng.dps.batch.impl.units.ESCities;
import com.echeng.dps.config.ESConstantConfig;
import com.echeng.dps.config.ESGlobalConfig;
import com.echeng.dps.config.ESParam;
import com.echeng.dps.config.ESParamConfig;
import com.echeng.dps.config.ESParamConfig.KEY_PARAM;
import com.echeng.dps.dict.ESDictRepository;
import com.echeng.dps.log.ESLog;
import com.echeng.dps.search.model.*;
import com.echeng.dps.search.model.unit.ESUnit;
import com.echeng.dps.search.querybuilder.ESJDProcess;
import com.echeng.dps.search.querybuilder.ESQueryBuilder;
import com.echeng.dps.search.querybuilder.ESResumeProcess;
import com.echeng.dps.search.rank.SortTypeCalculator;
import com.echeng.dps.search.searcher.ESSearcher;
import com.echeng.dps.search.server.handle.ESHandle;
import com.echeng.dps.tools.gmclient.QueryCorrectClient;
import com.echeng.dps.util.*;
import com.google.gson.reflect.TypeToken;
import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.stream.Collectors;

public class SearchHandleImpl extends ESHandle {
    private List<ESSearcher> searcherList;
    private Map<String, Map<String, ESSearcher>> searcherMap = new LinkedHashMap<String, Map<String, ESSearcher>>();
    private Map<String, String> specialParamMap = new LinkedHashMap<String, String>();
    private boolean inited = false;
    private static HashSet<String> functionTagParams = new HashSet<String>() {{
        add("all_function_tag");
        add("all_function_tag_attr");
        add("all_function_tag_qst");
        add("all_function_tag_should");
        add("first_function_tag");
        add("first_function_tag_attr");
        add("first_function_tag_extra");
        add("first_function_tag_extra_attr");
        add("function_tag");
        add("function_tag_extra");
        add("function_tag_loop");
        add("function_tag_loop1");
        add("function_tag_loop2");
        add("function_tag_loop3");
        add("function_tag_loop4");
        add("function_tag_noweight");
        add("function_tag_noweight_attr");
        add("function_tag_noweight_extra");
        add("function_tag_should");
        add("worked_function_tag");
        add("internal_all_function_tag");
        add("last_function_tag");
        add("worked_function_tag_extra");
    }};

    private BatchMgr batchMgr;

    public void setSpecialParamMap(Map<String, String> specialParamMap) {
        this.specialParamMap = specialParamMap;
    }

    public void setBatchMgr(BatchMgr batchMgr) {
        this.batchMgr = batchMgr;
    }

    public SearchHandleImpl(ESSearcher... searchers) {
        this.searcherList = new ArrayList<ESSearcher>();
        for (ESSearcher searcher : searchers) {
            this.searcherList.add(searcher);
        }
    }

    public ESSearcher getSearcher(String index, String tag) {
        Map<String, ESSearcher> map = searcherMap.getOrDefault(index, null);
        if (map == null)
            return null;
        return map.getOrDefault(tag, null);
    }

    @Override
    public void init() {
        synchronized (this) {
            if (!inited) {
                // dumpController.init();
                for (ESSearcher searcher : searcherList) {
                    searcher.init();
                    if (searcherMap.containsKey(searcher.getProductName())) {
                        searcherMap.get(searcher.getProductName()).put(searcher.getTag(), searcher);
                    } else {
                        Map<String, ESSearcher> map = new LinkedHashMap<String, ESSearcher>();
                        map.put(searcher.getTag(), searcher);
                        searcherMap.put(searcher.getProductName(), map);
                    }
                }
                batchMgr.initialize();
                ESDictRepository.getInstance();
                inited = true;
                new Thread(this).start();
            }
        }
        System.out.println(this.getName() + " is ready");
    }

    @Override
    public void close() {
        synchronized (this) {
            if (inited) {
                // dumpController.close();
                for (ESSearcher searcher : searcherList)
                    searcher.close();
                inited = false;
            }
        }
    }

    private void SearchResultProcess(ESResultSet result, ESRequest request, String tag, BatchMgr batchMgr, Map<String, List<String[]>> facetSearchParams) {
        if (request.containKey(ESConstantConfig.PARAM_FACET_ORIGINAL) && request.getParam(ESConstantConfig.PARAM_FACET_ORIGINAL).equals("1"))
            return;

        int facet_count = ESConstantConfig.FACET_DEAULT_SHOW_COUNT;
        if (request.getParams().containsKey("facet_count"))
            facet_count = Integer.valueOf(request.getParams().get("facet_count"));

        if (tag.equals("resume"))
            ESResumeProcess.facetProcess(result, request, batchMgr, facetSearchParams, facet_count);
        else if (tag.equals("jd"))
            ESJDProcess.facetProcess(result, request, batchMgr, facet_count);

        if (request.getFacetMode().size() > 0)
            result.setFacetMode(request.getFacetMode());

        if (request.getAmbiguousCorp().size() > 1)
            result.setAmbiguousCorp(request.getAmbiguousCorp());

        if (request.getAmbiguousFunc().size() > 1)
            result.setAmbiguousFunc(request.getAmbiguousFunc());

        if (request.getHighlight().has()) {
            // 添加空格分隔词汇
            if (ESStringUtil.isvalid(request.getOriginalKeyword()))
                request.getHighlight().addHighlight(Arrays.asList(request.getOriginalKeyword().split(" ")), batchMgr.getBatch().getDictionary());
            // 高亮结果处理
            request.getHighlight().normalize();
            //
            result.setHighlight(request.getHighlight());
        }

        if (request.getAnalysisList().size() > 0)
            result.setAnalysisList(request.getAnalysisList());
    }

    private void RecommendationResultProcess(ESResultSet result, ESRequest request, String tag, BatchMgr batchMgr, Map<String, List<String[]>> facetSearchParams) {
        if (request.getAmbiguousCorp().size() > 1)
            result.setAmbiguousCorp(request.getAmbiguousCorp());

        if (request.getAmbiguousFunc().size() > 1)
            result.setAmbiguousFunc(request.getAmbiguousFunc());

        if (request.getHighlight().has()) {
            // 添加空格分隔词汇
            if (ESStringUtil.isvalid(request.getOriginalKeyword()))
                request.getHighlight().addHighlight(Arrays.asList(request.getOriginalKeyword().split(" ")), batchMgr.getBatch().getDictionary());
            // 高亮结果处理
            request.getHighlight().normalize();
            //
            result.setHighlight(request.getHighlight());
        }

        if (request.getAnalysisList().size() > 0)
            result.setAnalysisList(request.getAnalysisList());
    }

    // 请求预处理
    private ESRequest preProcessRequest(ESRequest request) {
        request = parseLocation_weak(request);
        request = extendCorpkeyword(request);
        request = extendFunctionTag(request);
        request = parseNamePayload(request);
        request = extendDefaultParam(request);

        return request;
    }

    private ESRequest extendDefaultParam(ESRequest request) {
        if (request.getParam("index", "").equalsIgnoreCase("tobcv")) {
            if ((request.containKey("all_uids_public") || request.containKey("self_uids_public")) && !request.containKey("talent_is_deleted") && !request.containKey("importance_is_deleted_not")) {
                request.addParam("talent_is_deleted", "0");
            }
        }
        return request;
    }

    private ESRequest parseNamePayload(ESRequest request) {
        if (request.containKey("name_payload")) {
            request.addParam("name_payload", request.getParam("name_payload", "").replaceAll("\\s*", ""));
        }
        return request;
    }

    private ESRequest extendFunctionTag(ESRequest request) {
        Map<String, String> changedParams = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> entry : request.getParams().entrySet()) {
            if (functionTagParams.contains(entry.getKey())) {
                if (entry.getValue() == null || entry.getValue().isEmpty()) {
                    continue;
                }

                Set<Integer> ids = ESStringUtil.str2intset(entry.getValue());
                Set<Integer> newIds = ESDictRepository.getInstance().getFunctionTaxonomyMappingDict().mapToLatestFuncIds(ids);
                if (CollectionUtil.SetEquals(ids, newIds)) {
                    continue;
                }
                ids.addAll(newIds);
                changedParams.put(entry.getKey(), ESStringUtil.collectionString(ids, ",", true));
            }
        }
        if (!changedParams.isEmpty()) {
            ESLog.getInstance().info(this.getClass(), String.format("extendFunctionTag, before:%s", request.toString()));
            request.getParams().putAll(changedParams);
            ESLog.getInstance().info(this.getClass(), String.format("extendFunctionTag, after:%s", request.toString()));
        }

        return request;
    }

    private ESRequest extendCorpkeyword(ESRequest request) {
        try {
            if (request.containKey("corp_keyword")) {
                extendCorpkeywordInParam(request, "corp_keyword");
            }
            if (request.containKey("worked_corp_keyword")) {
                extendCorpkeywordInParam(request, "worked_corp_keyword");
            }
        } catch (Exception e) {
            ESLog.getInstance().warn(this.getClass(), "extendCorpkeyword error:" + e);
        }
        return request;
    }

    private void extendCorpkeywordInParam(ESRequest request, String param) {
        List<String> valueList = Arrays.asList(request.getParam(param).split(","));
        List<String> newValueList = new ArrayList<String>();
        for (String value : valueList) {
            if (ESDictRepository.getInstance().getCorpKeywordExtendDict().hasExtend(value)) {
                newValueList.addAll(ESDictRepository.getInstance().getCorpKeywordExtendDict().getExtend(value));
            } else {
                newValueList.add(value);
            }
        }
        request.getParams().put(param, StringUtils.join(newValueList, ","));
    }

    // TODO: 临时代码,前端location_weak未做省市处理,在此临时处理一下
    private ESRequest parseLocation_weak(ESRequest request) {
        try {
            if (!request.getParams().containsKey("location_weak"))
                return request;
            String location_weak = request.getParam("location_weak");
            ESCities cityDict = (ESCities) batchMgr.getBatch().getDictionary().get_cities();
            List<Integer> locations = cityDict.getLeafIds(ESStringUtil.StringCollection2IntList(Arrays.asList(location_weak.split(","))));
            request.getParams().put("location_weak", ESStringUtil.collectionString(locations, ",", false));
        } catch (Exception e) {
        }
        return request;
    }

    @Override
    public ESResponse assign(ESRequest requestIn) {
        TimeCalUtil tc = new TimeCalUtil();
        ESLog.getInstance().trace(this.getClass(), requestIn.getParams().toString());
        requestIn = preProcessRequest(requestIn);
        ESResponse response = new ESResponseAS(requestIn.getDetail() != null && requestIn.getDetail().equals("1"));
        response.setHeader(requestIn.getHeader());
        response.setIndex(batchMgr.getBatch().getId());
        // check validation
        if (!requestIn.isValid()) {
            return response;
        }
        String origSortType = requestIn.getParam("sort", "");
        ((ESResponseAS) response).setSortMode(origSortType);
        //
        String index = requestIn.getIndex();
        String tag = requestIn.getTag();
        if (tag.equals("resume")) {
            String keyword = requestIn.getParam("keyword", "");
            String correctKw = QueryCorrectClient.getInstance().QueryCorrect(keyword);
            ESLog.getInstance().info(this.getClass(), "[keyword]" + keyword + ";[correctKw]" + correctKw);
            Integer userId = Integer.valueOf(requestIn.getParam("uid", "0"));
            if (userId <= 0) {
                userId = Integer.valueOf(requestIn.getParam("pid", "0"));
            }

            String calculatedSortType = SortTypeCalculator.getSortType(userId, keyword, origSortType);

            if (origSortType.toLowerCase().trim().equals("dynamic")) {
                requestIn.addParam("sort", calculatedSortType);
            } else {
                calculatedSortType = origSortType;
            }

            ((ESResponseAS) response).setSortMode(calculatedSortType);
            if (calculatedSortType.toLowerCase().trim().equals("updated_at_day_desc") ||
                    calculatedSortType.toLowerCase().trim().equals("updated_at_desc")) {
                requestIn.addParam("sort", "updated_at_day_desc,field_score_desc");
            }
        }

        ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(tag) : null;
        if (searcher == null) {
            response.setParams(requestIn.getParams(), null);
            return response;
        }
        response.setParams(requestIn.getParams(), searcher.getParamConfig());
        ESParamConfig prs = searcher.getParamConfig();
        Analyzer analyzer = searcher.getQueryAnalyzer();

        ESRequest request = new ESRequest(requestIn);
        ESQuery query = getSearchQuery(request, prs, analyzer);

        long batchid = batchMgr.getBatch().getId();
        if (request.containKey("batchid")) {
            batchid = request.getParamInt("batchid");
        }

        ESResultSet result = new ESResultSet();
        /*如果存在keyword，但是keyword是meaningless的，则不search，其他情况需要search*/
        if (request.getAspectData() != null || (requestIn.getParam("keyword", "").equals("") && request.getAspectData() == null)) {

            tc.start();
            result = searcher.Search(request, query, batchid);
            tc.end();

			/*
			#1 对于query中的keyword是无效词这种情况，召回适量很大，相当于全库去除corpid！=0几个条件，在query构造过程中不好区分，从结果来考虑。
			对于召回比较多的，认为触发了上述的条件，初始化返回结果，对于接口返回召回数量变为0.
			#2 在解析的时候处理过了，这个逻辑暂时不考虑，先保留看下是不是有其他的问题，确定没有问题以后可以干掉

			if(result.getTotalHit() > 10000000){
				result = new ESResultSet();
			}
			*/
        }

        // Keyword Deletion
        int delection_count = Integer.valueOf(ESGlobalConfig.getInstance().getItem("kw_deleteion_max_times"));
        while (result.getTotalHit() == 0 && delection_count > 0) {
            String newKeyword = getKeywordAfterDeletion(request);
            if (newKeyword != null) {
                request = new ESRequest(requestIn);
                request.setOriginalKeyword(newKeyword);
                request.getParams().put(ESConstantConfig.PARAM_KEYWORD, newKeyword);
                ((ESResponseAS) response).setKeywordAfterDeleted(newKeyword);
                query = getSearchQuery(request, prs, analyzer);
                result = searcher.Search(request, query, batchMgr.getBatch().getId());
                if (result.getTotalHit() > 0)
                    break;
            }
            delection_count--;
        }
        // do some processing before return the result
        SearchResultProcess(result, request, tag, batchMgr, query.getFacetSearchParams());
        response.setResult(result);
        response.setParsedParams(request.getParams());

        return response;
    }

    public ESResponse assignForCampus(ESRequest requestIn, String resource) {
        TimeCalUtil tc = new TimeCalUtil();
        ESLog.getInstance().trace(this.getClass(), requestIn.getParams().toString());
        ESResponse response = new ESResponseAS(requestIn.getDetail() != null && requestIn.getDetail().equals("1"));
        response.setHeader(requestIn.getHeader());
        response.setIndex(batchMgr.getBatch().getId());
        if (!requestIn.isValid()) {
            return response;
        }
        String origSortType = requestIn.getParam("sort", "");
        ((ESResponseAS) response).setSortMode(origSortType);
        //
        String index = requestIn.getIndex();
        String tag = requestIn.getTag();

        ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(tag) : null;
        if (searcher == null) {
            response.setParams(requestIn.getParams(), null);
            return response;
        }
        response.setParams(requestIn.getParams(), searcher.getParamConfig());
        ESParamConfig prs = searcher.getParamConfig();
        Analyzer analyzer = searcher.getQueryAnalyzer();

        ESRequest request = new ESRequest(requestIn);
        ESQuery query = getCampusQuery(request, prs, analyzer);
        ESLog.getInstance().debug(this.getClass(), "query:" + query.toString(), false);

        long batchid = batchMgr.getBatch().getId();
        if (request.containKey("batchid")) {
            batchid = request.getParamInt("batchid");
        }

        tc.start();
        ESResultSet result = searcher.RecommendationSearch(request, query, batchid, resource, null);
        tc.end();


        // do some processing before return the result
        SearchResultProcess(result, request, tag, batchMgr, query.getFacetSearchParams());
        response.setResult(result);
        response.setParsedParams(request.getParams());

        return response;
    }

    public ESResponse assignForCVRecommendation(ESRequest requestIn, String resource, String matchType, List<String> timeCost) {
        TimeCalUtil tc = new TimeCalUtil();
        tc.start();
        ESLog.getInstance().trace(this.getClass(), requestIn.getParams().toString());
        requestIn = preProcessRequest(requestIn);
        ESResponse response = new ESResponseAS(requestIn.getDetail() != null && requestIn.getDetail().equals("1"));
        response.setHeader(requestIn.getHeader());
        response.setIndex(batchMgr.getBatch().getId());
        // check validation
        if (!requestIn.isValid()) {
            return response;
        }
        String index = requestIn.getIndex();
        String tag = requestIn.getTag();

        String origSortType = requestIn.getParam("sort", "");
        ((ESResponseAS) response).setSortMode(origSortType);

        ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(tag) : null;
        if (searcher == null) {
            response.setParams(requestIn.getParams(), null);
            return response;
        }
        response.setParams(requestIn.getParams(), searcher.getParamConfig());
        ESParamConfig prs = searcher.getParamConfig();
        Analyzer analyzer = searcher.getQueryAnalyzer();

        ESRequest request = new ESRequest(requestIn);

        ESQuery query = getCVRecommendationQuery(request, prs, analyzer);

        long batchid = batchMgr.getBatch().getId();
        if (request.containKey("batchid")) {
            batchid = request.getParamInt("batchid");
        }
        tc.end();
        timeCost.add(matchType + ":" + resource + "BeforeSearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");


        tc.start();
        ESResultSet result = searcher.RecommendationSearch(request, query, batchid, resource, null);
        tc.end();
        timeCost.add(matchType + ":" + resource + "SearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");

        tc.start();
        // do some processing before return the result
        RecommendationResultProcess(result, request, tag, batchMgr, query.getFacetSearchParams());
        response.setResult(result);
        response.setParsedParams(request.getParams());

        tc.end();
        timeCost.add(matchType + ":" + resource + "AfterSearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");

        return response;

    }

    public ESResponse assignForRecommendationParallel(ESRequest requestIn, String resource, String matchType,
                                                      String preference, List<String> timeCost) {
        TimeCalUtil tc = new TimeCalUtil();
        tc.start();
        ESLog.getInstance().trace(this.getClass(), requestIn.getParams().toString());
        requestIn = preProcessRequest(requestIn);  // 预处理request
        ESResponse response = new ESResponseAS(requestIn.getDetail() != null && requestIn.getDetail().equals("1"));
        response.setHeader(requestIn.getHeader());
        response.setIndex(batchMgr.getBatch().getId());
        // check validation
        if (!requestIn.isValid()) {
            return response;
        }
        String index = requestIn.getIndex();
        String tag = requestIn.getTag();

        String origSortType = requestIn.getParam("sort", "");
        ((ESResponseAS) response).setSortMode(origSortType);
        if (tag.equals("resume")) {
            String keyword = requestIn.getParam("keyword", "");
            Integer userId = Integer.valueOf(requestIn.getParam("uid", "0"));
            if (userId <= 0) {
                userId = Integer.valueOf(requestIn.getParam("pid", "0"));
            }

            String calculatedSortType = SortTypeCalculator.getSortType(userId, keyword, origSortType);

            if (origSortType.toLowerCase().trim().equals("dynamic")) {
                requestIn.addParam("sort", calculatedSortType);
            } else {
                calculatedSortType = origSortType;
            }

            ((ESResponseAS) response).setSortMode(calculatedSortType);
            if (calculatedSortType.toLowerCase().trim().equals("updated_at_day_desc") ||
                    calculatedSortType.toLowerCase().trim().equals("updated_at_desc")) {
                requestIn.addParam("sort", "updated_at_day_desc, field_score_desc");
            }
        }

        ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(tag) : null;
        if (searcher == null) {
            response.setParams(requestIn.getParams(), null);
            return response;
        }
        response.setParams(requestIn.getParams(), searcher.getParamConfig());
        ESParamConfig prs = searcher.getParamConfig();
        Analyzer analyzer = searcher.getQueryAnalyzer();

        ESRequest request = new ESRequest(requestIn);

        ESQuery query = getRecommendationQuery(request, prs, analyzer);
        //ESLog.getInstance().info(getClass(), "test get jd_input:"+query.getQuery());
        long batchid = batchMgr.getBatch().getId();
        if (request.containKey("batchid")) {
            batchid = request.getParamInt("batchid");
        }
        tc.end();
        timeCost.add(matchType + ":" + resource + "BeforeSearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");


        tc.start();
        ESResultSet result = searcher.RecommendationSearch(request, query, batchid, resource, preference);
        tc.end();
        timeCost.add(matchType + ":" + resource + "SearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");

        tc.start();
        // do some processing before return the result
        RecommendationResultProcess(result, request, tag, batchMgr, query.getFacetSearchParams());
        response.setResult(result);
        response.setParsedParams(request.getParams());

        tc.end();
        timeCost.add(matchType + ":" + resource + "AfterSearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");

        return response;
    }

    public ESResponse assignForRecommendation(ESRequest requestIn, String resource, String matchType, List<String> timeCost) {
        TimeCalUtil tc = new TimeCalUtil();
        tc.start();
        ESLog.getInstance().trace(this.getClass(), requestIn.getParams().toString());
        requestIn = preProcessRequest(requestIn);  // 预处理request
        ESResponse response = new ESResponseAS(requestIn.getDetail() != null && requestIn.getDetail().equals("1"));
        response.setHeader(requestIn.getHeader());
        response.setIndex(batchMgr.getBatch().getId());
        // check validation
        if (!requestIn.isValid()) {
            return response;
        }
        String index = requestIn.getIndex();
        String tag = requestIn.getTag();

        String origSortType = requestIn.getParam("sort", "");
        ((ESResponseAS) response).setSortMode(origSortType);
        if (tag.equals("resume")) {
            String keyword = requestIn.getParam("keyword", "");
            Integer userId = Integer.valueOf(requestIn.getParam("uid", "0"));
            if (userId <= 0) {
                userId = Integer.valueOf(requestIn.getParam("pid", "0"));
            }

            String calculatedSortType = SortTypeCalculator.getSortType(userId, keyword, origSortType);

            if (origSortType.toLowerCase().trim().equals("dynamic")) {
                requestIn.addParam("sort", calculatedSortType);
            } else {
                calculatedSortType = origSortType;
            }

            ((ESResponseAS) response).setSortMode(calculatedSortType);
            if (calculatedSortType.toLowerCase().trim().equals("updated_at_day_desc") ||
                    calculatedSortType.toLowerCase().trim().equals("updated_at_desc")) {
                requestIn.addParam("sort", "updated_at_day_desc, field_score_desc");
            }
        }

        ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(tag) : null;
        if (searcher == null) {
            response.setParams(requestIn.getParams(), null);
            return response;
        }
        response.setParams(requestIn.getParams(), searcher.getParamConfig());
        ESParamConfig prs = searcher.getParamConfig();
        Analyzer analyzer = searcher.getQueryAnalyzer();

        ESRequest request = new ESRequest(requestIn);
        ESQuery query = getRecommendationQuery(request, prs, analyzer);
        long batchid = batchMgr.getBatch().getId();
        if (request.containKey("batchid")) {
            batchid = request.getParamInt("batchid");
        }
        tc.end();
        timeCost.add(matchType + ":" + resource + "BeforeSearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");


        tc.start();
        ESResultSet result = searcher.RecommendationSearch(request, query, batchid, resource, null);
        tc.end();
        timeCost.add(matchType + ":" + resource + "SearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");

        tc.start();
        // do some processing before return the result
        RecommendationResultProcess(result, request, tag, batchMgr, query.getFacetSearchParams());
        response.setResult(result);
        response.setParsedParams(request.getParams());

        tc.end();
        timeCost.add(matchType + ":" + resource + "AfterSearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");

        return response;
    }

    public ESQuery getRecommendationQuery(ESRequest request, ESParamConfig prs, Analyzer analyzer) {

        ESQuery esQuery = new ESQuery();
        Object o;
        ESParam pr;

        pr = prs.getParam(KEY_PARAM.count.toString());
        o = request.get(KEY_PARAM.count.toString(), pr);
        if (o != null) {
            int count = (int) o;
            if (count >= 1)
                esQuery.setCount(count);
        }


        pr = prs.getParam(KEY_PARAM.spid.toString());
        o = request.get(KEY_PARAM.spid.toString(), pr);
        ESUnit unit = null;
        if (o != null && ((int) o) >= 0) {
            int spid = (int) o;
            String index = request.getIndex();
            String tag = request.getTag();
            ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(specialParamMap.get(tag)) : null;
            unit = searcher.getUnit(request, spid, batchMgr.getBatch().getId());    //值得注意的地方
        }

        // resume 预处理
        if (request.getTag().equals("resume"))
            ESResumeProcess.paramPreProcess(request, batchMgr, analyzer);
        else if (request.getTag().equals("jd"))
            ESJDProcess.paramPreProcess(request, batchMgr, analyzer);
        // 设置排序和facet参数
        esQuery.setSortFieldsArray(getSortField(request, prs));
        esQuery.setFacetSearchParams(getFacetParams(request, prs));

        if (request.getParams().containsKey("facet_count"))
            esQuery.setFacet_count(Integer.valueOf(request.getParams().get("facet_count")));

        // 开始拼Query
        BoolQueryBuilder bquery = QueryBuilders.boolQuery();
        // 解析非关键词
        Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>();
        bquery = ESQueryBuilder.buildBooleanQuery(bquery, request, prs, analyzer, unit, batchMgr, attrQueryMap);

        // 解析特殊公司职级
        QueryBuilder specCorpRankQuery = ESQueryBuilder.buildSpecialCorpRankQuery(request, prs, analyzer, unit, batchMgr);
        if (specCorpRankQuery != null)
            bquery.must(specCorpRankQuery);

        QueryBuilder query = bquery;

        // 添加FILTER
        FilterBuilder filter = ESQueryBuilder.buildFilterQuery(request, prs, analyzer, unit, batchMgr);
        if (filter != null) {
            query = QueryBuilders.filteredQuery(query, filter);
        }
        // 配置属性
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWSCORE))
            esQuery.setExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_DETAILSCORE))
            esQuery.setDetailExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWQUERY))
            esQuery.setNeedQueryDetail(true);
        //if (request.getParams().containsKey(ESConstantConfig.PARAM_CORP_FUNC_CNT))
        //	esQuery.setNeedCorpfuncCnt(true);
        if (request.keyIsTrue(ESConstantConfig.PARAM_RESULT_LIMIT))
            esQuery.setLimit(true);

        esQuery.setQuery(query);
        esQuery.setAttrQueryMap(attrQueryMap);

        return esQuery;
    }

    private ESQuery getCVRecommendationQuery(ESRequest request, ESParamConfig prs, Analyzer analyzer) {
        ESQuery esQuery = new ESQuery();
        Object o;
        ESParam pr;

        pr = prs.getParam(KEY_PARAM.count.toString());
        o = request.get(KEY_PARAM.count.toString(), pr);
        if (o != null) {
            int count = (int) o;
            if (count >= 1)
                esQuery.setCount(count);
        }


        pr = prs.getParam(KEY_PARAM.spid.toString());
        o = request.get(KEY_PARAM.spid.toString(), pr);
        ESUnit unit = null;

        ESJDProcess.paramPreProcess(request, batchMgr, analyzer);
        // 设置排序和facet参数
        esQuery.setSortFieldsArray(getSortField(request, prs));
        esQuery.setFacetSearchParams(getFacetParams(request, prs));

        // 开始拼Query
        BoolQueryBuilder bquery = QueryBuilders.boolQuery();
        // 解析非关键词
        Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>();
        bquery = ESQueryBuilder.buildBooleanQuery(bquery, request, prs, analyzer, unit, batchMgr, attrQueryMap);

        // 解析特殊公司职级
        QueryBuilder specCorpRankQuery = ESQueryBuilder.buildSpecialCorpRankQuery(request, prs, analyzer, unit, batchMgr);
        if (specCorpRankQuery != null)
            bquery.must(specCorpRankQuery);

        QueryBuilder query = ESJDProcess.queryPostProcess(bquery, request, prs, analyzer, unit, batchMgr);

        // 添加FILTER
        FilterBuilder filter = ESQueryBuilder.buildFilterQuery(request, prs, analyzer, unit, batchMgr);

        if (filter != null) {
            query = QueryBuilders.filteredQuery(query, filter);
        }
        // 配置属性
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWSCORE))
            esQuery.setExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_DETAILSCORE))
            esQuery.setDetailExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWQUERY))
            esQuery.setNeedQueryDetail(true);
        //if (request.getParams().containsKey(ESConstantConfig.PARAM_CORP_FUNC_CNT))
        //	esQuery.setNeedCorpfuncCnt(true);
        if (request.keyIsTrue(ESConstantConfig.PARAM_RESULT_LIMIT))
            esQuery.setLimit(true);

        esQuery.setQuery(query);
        esQuery.setAttrQueryMap(attrQueryMap);

        return esQuery;
    }

    private ESQuery getSearchQuery(ESRequest request, ESParamConfig prs, Analyzer analyzer) {
        ESQuery esQuery = new ESQuery();
        Object o;
        ESParam pr;
        pr = prs.getParam(KEY_PARAM.start.toString());
        o = request.get(KEY_PARAM.start.toString(), pr);
        if (o != null) {
            int start = (int) o;
            if (start >= 0)
                esQuery.setStart(start);
        }

        pr = prs.getParam(KEY_PARAM.count.toString());
        o = request.get(KEY_PARAM.count.toString(), pr);
        if (o != null) {
            int count = (int) o;
            if (count >= 1)
                esQuery.setCount(count);
        }

        pr = prs.getParam(KEY_PARAM.spid.toString());
        o = request.get(KEY_PARAM.spid.toString(), pr);
        ESUnit unit = null;
        if (o != null && ((int) o) >= 0) {
            int spid = (int) o;
            String index = request.getIndex();
            String tag = request.getTag();
            ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(specialParamMap.get(tag)) : null;
            unit = searcher.getUnit(request, spid, batchMgr.getBatch().getId());
        }

        // resume 预处理
        if (request.getTag().equals("resume"))
            ESResumeProcess.paramPreProcess(request, batchMgr, analyzer);
        else if (request.getTag().equals("jd"))
            ESJDProcess.paramPreProcess(request, batchMgr, analyzer);
        // 设置排序和facet参数
        esQuery.setSortFieldsArray(getSortField(request, prs));
        esQuery.setFacetSearchParams(getFacetParams(request, prs));

        if (request.getParams().containsKey("facet_count"))
            esQuery.setFacet_count(Integer.valueOf(request.getParams().get("facet_count")));

        // 开始拼Query
        BoolQueryBuilder bquery = QueryBuilders.boolQuery();
        // 解析非关键词
        Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>();
        bquery = ESQueryBuilder.buildBooleanQuery(bquery, request, prs, analyzer, unit, batchMgr, attrQueryMap);
        // 解析关键词
        bquery = ESQueryBuilder.buildKeywordQuery(bquery, request, prs, analyzer, batchMgr);
        // 解析父子query
        bquery = ESQueryBuilder.buildChildQuery(bquery, request, prs, analyzer, batchMgr);

        // 解析特殊公司职级
        QueryBuilder specCorpRankQuery = ESQueryBuilder.buildSpecialCorpRankQuery(request, prs, analyzer, unit, batchMgr);
        if (specCorpRankQuery != null)
            bquery.must(specCorpRankQuery);

        // Query后处理,添加附加逻辑
        QueryBuilder query = bquery;

        if (request.getTag().equals("resume"))
            query = ESResumeProcess.queryPostProcess(bquery, request, prs, analyzer, unit, batchMgr);
        else if (request.getTag().equals("jd"))
            query = ESJDProcess.queryPostProcess(bquery, request, prs, analyzer, unit, batchMgr);

        // 临时代码, 纯hard coding
        if (request.getTag().equals("resume"))
            query = ESResumeProcess.tempManualProcess(query, request, prs, analyzer, unit, batchMgr);

        // 添加FILTER
        BoolFilterBuilder filter = ESQueryBuilder.buildFilterQuery(request, prs, analyzer, unit, batchMgr);

        if (filter != null && filter.hasClauses()) {
            query = QueryBuilders.filteredQuery(query, filter);
        }
        // 配置属性
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWSCORE))
            esQuery.setExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_DETAILSCORE))
            esQuery.setDetailExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWQUERY))
            esQuery.setNeedQueryDetail(true);
        //if (request.getParams().containsKey(ESConstantConfig.PARAM_CORP_FUNC_CNT))
        //	esQuery.setNeedCorpfuncCnt(true);
        if (request.keyIsTrue(ESConstantConfig.PARAM_RESULT_LIMIT))
            esQuery.setLimit(true);

        esQuery.setQuery(query);
        esQuery.setAttrQueryMap(attrQueryMap);

        return esQuery;
    }

    private ESQuery getCampusQuery(ESRequest request, ESParamConfig prs, Analyzer analyzer) {
        ESQuery esQuery = new ESQuery();
        Object o;
        ESParam pr;
        esQuery.setStart(0);
        esQuery.setCount(3000);

        pr = prs.getParam(KEY_PARAM.spid.toString());
        o = request.get(KEY_PARAM.spid.toString(), pr);
        ESUnit unit = null;
        if (o != null && ((int) o) >= 0) {
            int spid = (int) o;
            String index = request.getIndex();
            String tag = request.getTag();
            ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(specialParamMap.get(tag)) : null;
            unit = searcher.getUnit(request, spid, batchMgr.getBatch().getId());
        }

        // 开始拼Query
        BoolQueryBuilder bquery = QueryBuilders.boolQuery();
        // 解析非关键词
        Map<String, QueryBuilder> attrQueryMap = new HashMap<String, QueryBuilder>();
        bquery = ESQueryBuilder.buildCampusBooleanQuery(bquery, request, prs, analyzer, unit, batchMgr, attrQueryMap);

        // Query后处理,添加附加逻辑
        QueryBuilder query = bquery;

        // 添加FILTER
        FilterBuilder filter = ESQueryBuilder.buildCampusFilterQuery(request, prs, analyzer, unit, batchMgr);

        if (filter != null) {
            query = QueryBuilders.filteredQuery(query, filter);
        }
        // 配置属性
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWSCORE))
            esQuery.setExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_DETAILSCORE))
            esQuery.setDetailExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWQUERY))
            esQuery.setNeedQueryDetail(true);
        //if (request.getParams().containsKey(ESConstantConfig.PARAM_CORP_FUNC_CNT))
        //	esQuery.setNeedCorpfuncCnt(true);
        if (request.keyIsTrue(ESConstantConfig.PARAM_RESULT_LIMIT))
            esQuery.setLimit(true);

        esQuery.setQuery(query);
        esQuery.setAttrQueryMap(attrQueryMap);

        return esQuery;
    }

    private List<SortBuilder> getSortField(ESRequest request, ESParamConfig prs) {
        SortBuilder defaultSort = SortBuilders.scoreSort();
        List<SortBuilder> sf = new ArrayList<SortBuilder>();
        sf.add(defaultSort);

        ESParam pr = prs.getParam(KEY_PARAM.sort.toString());
        String sortby = (String) request.get(KEY_PARAM.sort.toString(), pr);
        if (sortby == null || sortby.length() <= 0) {
            if (request.getParams().containsKey(ESConstantConfig.PARAM_MANUAL_SORT))
                sortby = request.getParams().get(ESConstantConfig.PARAM_MANUAL_SORT);
            if (sortby == null)
                return sf;
        }

        List<SortBuilder> sortList = new ArrayList<SortBuilder>();
        boolean reverse = false;
        String[] sortstrs = sortby.split(",");
        String fieldname = "";
        boolean useScore = false;
        for (String sortstr : sortstrs) {
            sortstr = sortstr.trim();
            if (sortstr.endsWith(ESConstantConfig.SORT_DESC)) {
                reverse = true;
                fieldname = sortstr.substring(0, sortstr.indexOf(ESConstantConfig.SORT_DESC));
            } else if (sortstr.endsWith(ESConstantConfig.SORT_ASC)) {
                reverse = false;
                fieldname = sortstr.substring(0, sortstr.indexOf(ESConstantConfig.SORT_ASC));
            } else {
                reverse = false;
                fieldname = sortstr;
            }

            SortBuilder sortField = null;
            if (fieldname.equals(ESConstantConfig.PARAM_FIELD_SCORE)) {
                reverse = true;
                sortField = SortBuilders.scoreSort();
                useScore = true;
            } else if (fieldname.equals(ESConstantConfig.PARAM_FIELD_RANDOM)) {
                sortField = SortBuilders.scriptSort("random()", "number");
            } else if (fieldname.equals(ESConstantConfig.PARAM_FIELD_DATE_SORT)) {
                String script = "(doc['updated_at'].value/" + ESConstantConfig.DATESORT_DIVIDEND + ") as int";
                sortField = SortBuilders.scriptSort(script, "number");
            } else if (fieldname.equals(ESConstantConfig.PARAM_FIELD_LASTDATE_SORT)) {
                String script = "(doc['last_updated_at'].value/" + ESConstantConfig.DATESORT_DIVIDEND + ") as int";
                sortField = SortBuilders.scriptSort(script, "number");
            } else {
                // System.out.println(fieldname);
                ESParam sortpr = prs.getParam(fieldname);
                if (sortpr == null || sortpr.getChildType() != null) {
                    continue;
                }
                String realFieldName = sortpr.getFieldname();
                if (realFieldName == null || realFieldName.length() <= 0)
                    continue;
                sortField = SortBuilders.fieldSort(realFieldName);
            }
            sortField.order(reverse ? SortOrder.DESC : SortOrder.ASC);
            sortList.add(sortField);
        }

        if (sortList.size() == 0)
            return sf;
        else {
            if (!useScore)
                sortList.add(defaultSort);
            return sortList;
        }
    }

    private Map<String, List<String[]>> getFacetParams(ESRequest request, ESParamConfig prs) {
        Map<String, String> paramMap2 = request.getParams();
        ESParam pr = prs.getParam(KEY_PARAM.facet.toString());
        String paramStr = ((String) request.get(KEY_PARAM.facet.toString(), pr));
        if (paramStr == null || paramStr.length() <= 0) {
            pr = prs.getParam(KEY_PARAM.facet_ext.toString());
            paramStr = ((String) request.get(KEY_PARAM.facet_ext.toString(), pr));
            if (paramStr == null || paramStr.length() <= 0)
                return null;
        }

        paramStr = paramStr.toLowerCase();
        Map<String, List<String[]>> ret = new HashMap<String, List<String[]>>();

        String[] facetstrs = paramStr.split(",");
        for (String s : facetstrs) {
            String[] ss = s.split(ESConstantConfig.FACET_DELIMITER_STR);
            String fieldname = ss[0];

            ESParam pr_field = prs.getFieldParam(fieldname);
            if (pr_field == null)
                continue;
            String realFieldName = pr_field.getFieldname();
            if (realFieldName == null || realFieldName.length() <= 0)
                continue;

            List<String[]> facetList = new ArrayList<String[]>();
            String valstr = paramMap2.get(fieldname + ESConstantConfig.FACET_SUFFIX);

            String[] vals = new String[0];
            if (valstr != null)
                vals = valstr.split(",");

            List<String> tempVals = new ArrayList<String>();
            if (pr_field.getName().equals("work_experience")) {
                for (String val : vals) {
                    try {
                        LongRangeType w = LongRangeType.valueOf(val);
                        if (tempVals == null)
                            tempVals = new ArrayList<String>();
                        for (long i = w.getMin().longValue(); i < w.getMax().longValue() + 1; i++)
                            tempVals.add(String.valueOf(i));
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }

            if (tempVals != null) {
                vals = tempVals.toArray(new String[tempVals.size()]);
            }

            int subsize = vals.length > 0 ? ss.length : ss.length - 1;
            String[] sub = new String[subsize];
            if (ss.length > 1)
                System.arraycopy(ss, 1, sub, 0, ss.length - 1);

            if (vals.length > 0) {
                for (int i = 0; i < vals.length; i++) {
                    String[] subclone = sub.clone();
                    System.arraycopy(vals, i, subclone, ss.length - 1, 1);
                    facetList.add(subclone);
                }
            } else {
                facetList.add(sub);
            }

            ret.put(fieldname, facetList);
        }
        return ret;
    }

    private String getKeywordAfterDeletion(ESRequest request) {
        if (request.getKwMap().size() > 1 && request.getOriginalKeyword() != null
                && request.getParams().containsKey(ESParamConfig.KEY_PARAM.kw_deletion.toString())
                && request.getParam(ESParamConfig.KEY_PARAM.kw_deletion.toString()).equals("1")) {
            Map<String, Float> kwMap = request.getKwMap();
            String oldKeyword = request.getOriginalKeyword();
            String newKeyword = "";
            TreeMap<Float, String> weightMap = new TreeMap<Float, String>();
            for (Map.Entry<String, Float> e : kwMap.entrySet()) {
                weightMap.put(e.getValue(), e.getKey());
            }
            String deleteKey = weightMap.get(weightMap.firstKey());
            int index = oldKeyword.indexOf(deleteKey);
            if (index != -1) {
                String sub1 = oldKeyword.substring(0, index);
                String sub2 = oldKeyword.substring(index + deleteKey.length(), oldKeyword.length());
                newKeyword = sub1 + sub2;
                return newKeyword;
            }
        }
        return null;
    }

    // switch newest index
    @Override
    public void run() {
        int BATCH_DELAY = Integer.parseInt(ESGlobalConfig.getInstance().getItem("es_sleep_duration")) * 1000;
        while (true) {
            try {
                batchMgr.updateToNewest();
                Thread.sleep(BATCH_DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
                try {
                    Thread.sleep(BATCH_DELAY);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public ESResponse getCvDataByRequest(ESRequest requestIn, List<String> timeCost){
        TimeCalUtil tc = new TimeCalUtil();
        ESLog.getInstance().trace(this.getClass(), requestIn.getParams().toString());
        ESResponse response = new ESResponseAS(requestIn.getDetail() != null && requestIn.getDetail().equals("1"));
        response.setHeader(requestIn.getHeader());
        response.setIndex(batchMgr.getBatch().getId());
        // check validation
        if (!requestIn.isValid()) {
            return response;
        }
        String index = requestIn.getIndex();
        String tag = requestIn.getTag();

        ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(tag) : null;
        if (searcher == null) {
            response.setParams(requestIn.getParams(), null);
            return response;
        }
        response.setParams(requestIn.getParams(), searcher.getParamConfig());

        ESParamConfig prs = searcher.getParamConfig();
        Analyzer analyzer = searcher.getQueryAnalyzer();

        long batchid = batchMgr.getBatch().getId();
        if (requestIn.containKey("batchid")) {
            batchid = requestIn.getParamInt("batchid");
        }

        ESRequest request = new ESRequest(requestIn);
        ESResultSet result = null;
        ESQuery query = null;

        Map<String, Object> esTypeMapping = searcher.searchMapping(request, batchid, request.getResource());
        Map<String, Object> fieldsMapping = (HashMap<String, Object>)esTypeMapping.get("properties");
        List<String> fieldList = new ArrayList<>();
        fieldList.addAll(fieldsMapping.keySet());
        fieldList.remove("relations");
        request.setEsRetFieldNames(fieldList);

        String json = request.getParam("cv_ids");
        List<Long> cvIds = (List<Long>) JsonUtil.fromJson(json, new TypeToken<List<Long>>(){}.getType());
        if (cvIds == null){
            cvIds = new ArrayList<Long>();
        }

        ESQuery idsQuery = getIdsQuery(request, prs, analyzer, cvIds);


        tc.start();
        result = searcher.idSearch(request, idsQuery, batchid, request.getResource());
        tc.end();
        timeCost.add("getCvData SearchIndexCost:" + Long.toString(tc.getDuration()) + "ms");


        // do some processing before return the result
        response.setResult(result);
        response.setParsedParams(request.getParams());

        return response;
    }

    public ESQuery getIdsQuery(ESRequest request, ESParamConfig prs, Analyzer analyzer, List<Long> ids) {

        ESQuery esQuery = new ESQuery();
        Object o;
        ESParam pr;

        pr = prs.getParam(KEY_PARAM.count.toString());
        o = request.get(KEY_PARAM.count.toString(), pr);
        if (o != null) {
            int count = (int) o;
            if (count >= 1)
                esQuery.setCount(count);
        }


        pr = prs.getParam(KEY_PARAM.spid.toString());
        o = request.get(KEY_PARAM.spid.toString(), pr);
        ESUnit unit = null;
        if (o != null && ((int) o) >= 0) {
            int spid = (int) o;
            String index = request.getIndex();
            String tag = request.getTag();
            ESSearcher searcher = searcherMap.containsKey(index) ? searcherMap.get(index).get(specialParamMap.get(tag)) : null;
            unit = searcher.getUnit(request, spid, batchMgr.getBatch().getId());
        }

        // resume 预处理
        if (request.getTag().equals("resume"))
            ESResumeProcess.paramPreProcess(request, batchMgr, analyzer);
        else if (request.getTag().equals("jd"))
            ESJDProcess.paramPreProcess(request, batchMgr, analyzer);
        // 设置排序和facet参数
        esQuery.setSortFieldsArray(getSortField(request, prs));
        esQuery.setFacetSearchParams(getFacetParams(request, prs));

        if (request.getParams().containsKey("facet_count"))
            esQuery.setFacet_count(Integer.valueOf(request.getParams().get("facet_count")));

        // 配置属性
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWSCORE))
            esQuery.setExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_DETAILSCORE))
            esQuery.setDetailExplain(true);
        if (request.getParams().containsKey(ESConstantConfig.PARAM_SHOWQUERY))
            esQuery.setNeedQueryDetail(true);
        //if (request.getParams().containsKey(ESConstantConfig.PARAM_CORP_FUNC_CNT))
        //	esQuery.setNeedCorpfuncCnt(true);
        if (request.keyIsTrue(ESConstantConfig.PARAM_RESULT_LIMIT))
            esQuery.setLimit(true);


        List<String> idsStrList = ids.stream()
                .map(s -> String.valueOf(s))
                .collect(Collectors.toList());

        String[] idsStrArr = idsStrList.toArray(new String[idsStrList.size()]);
        QueryBuilder query = QueryBuilders.idsQuery().ids(idsStrArr);


        esQuery.setQuery(query);

        return esQuery;
    }
}
