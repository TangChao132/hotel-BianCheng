package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.dto.RequestParams;
import cn.itcast.hotel.pojo.enity.Hotel;
import cn.itcast.hotel.pojo.vo.HotelDoc;
import cn.itcast.hotel.pojo.vo.PageResult;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {

            //           准备request
            SearchRequest request = new SearchRequest("hotel");
            //        准备Query参数
            buildBasicRequest(params, request);

            //        解析请求参数
            int page = params.getPage();
            int size = params.getSize();
            //        分页处理
            request.source().from((page - 1) * size).size(size);

            //排序
            String location = params.getLocation();
            if (location!=null && !location.equals("")){
                request.source().sort(SortBuilders.geoDistanceSort("location",new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS));
            }

            //发送请求，得到响应结果
            SearchResponse response= client.search(request, RequestOptions.DEFAULT);
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //解析结果

    }

    private static void buildBasicRequest(RequestParams params, SearchRequest request) {
        //1.原始查询
        String key = params.getKey();

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (key == null || key.equals("")) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        //城市条件
        if (params.getCity() != null && !params.getCity().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        //品牌条件
        if (params.getBrand() != null && !params.getBrand().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        //星级条件
        if (params.getStarName() != null && !params.getStarName().equals("")) {
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        //价格
        if (params.getMaxPrice() != null && params.getMinPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").lte(params.getMaxPrice()).gte(params.getMinPrice()));
        }
        //2.算分查询
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        //原始查询
                        boolQuery,
                        //function score的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                //其中一个function score 元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        //过滤条件
                                        QueryBuilders.termQuery("isAD",true),
                                        //算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        request.source().query(boolQuery);
    }

    private static PageResult handleResponse(SearchResponse response) {
        //解析结果
        SearchHits searchHits = response.getHits();
        //输出查询文档总数
        long total = searchHits.getTotalHits().value;

        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> list = new ArrayList<>();
        for (SearchHit hit : hits) {
//            获取文档source
            String json = hit.getSourceAsString();
            Object[] sortValues = hit.getSortValues();
//            反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            if (sortValues.length>0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            list.add(hotelDoc);
        }

        return new PageResult(list, total);
    }
    @Override
    public Map<String, List<String>> filters(RequestParams Params) {
        try {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");

        buildBasicRequest(Params,request);
        //2.1设置size
        request.source().size(0);
        //2.2聚合
        buildAggregation(request);
        //3.发出请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.1解析结果
        Map<String, List<String>> result=new HashMap<>();
        Aggregations aggregations = response.getAggregations();
        List<String> brandList = getAggByName(aggregations,"brandAgg");
            result.put("brand",brandList);
        List<String> cityList = getAggByName(aggregations,"cityAgg");
            result.put("city",cityList);
        List<String> starList = getAggByName(aggregations,"starAgg");
            result.put("starName",starList);
            System.out.println(result.get("brand"));
            System.out.println(result.get("city"));
            System.out.println(result.get("starName"));
        return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestion(String prefix) {
        try {
            SearchRequest request = new SearchRequest("hotel");
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "mySuggestion",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .prefix(prefix)
                            .skipDuplicates(true)
                            .size(10)
            ));
            SearchResponse response= client.search(request, RequestOptions.DEFAULT);

            CompletionSuggestion suggestion = response.getSuggest().getSuggestion("mySuggestion");
            List<CompletionSuggestion.Entry.Option> options = suggestion.getOptions();
            List<String> list=new ArrayList<>(options.size());
            for (CompletionSuggestion.Entry.Option option :options){
                String text = option.getText().toString();
                list.add(text);
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            // 1.准备Request
            DeleteRequest request = new DeleteRequest("hotel", id.toString());
            // 2.发送请求
            client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        try {
            // 0.根据id查询酒店数据
            Hotel hotel = getById(id);
            // 转换为文档类型
            HotelDoc hotelDoc = new HotelDoc(hotel);

            // 1.准备Request对象
            IndexRequest request = new IndexRequest("hotel").id(id.toString());
            // 2.准备Json文档
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            // 3.发送请求
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static List<String> getAggByName(Aggregations aggregations,String aggName) {
        Terms brandTerms= aggregations.get(aggName);
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        List<String> brandList = new ArrayList<>();
        //4.2遍历集合
        for (Terms.Bucket bucket :buckets){
            //4.3获取key(酒店名称)
            String key = bucket.getKeyAsString();
            brandList.add(key);
        }
        return brandList;
    }

    private static void buildAggregation(SearchRequest request) {
        request.source().aggregation(
                AggregationBuilders
                        .terms("brandAgg")
                        .field("brand")
                        .size(100)
        );
        request.source().aggregation(
                AggregationBuilders
                        .terms("cityAgg")
                        .field("city")
                        .size(100)
        );
        request.source().aggregation(
                AggregationBuilders
                        .terms("starAgg")
                        .field("starName")
                        .size(100)
        );
    }
}
