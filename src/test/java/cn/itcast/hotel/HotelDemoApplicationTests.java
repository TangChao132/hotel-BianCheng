package cn.itcast.hotel;

import cn.itcast.hotel.pojo.enity.Hotel;
import cn.itcast.hotel.pojo.vo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.*;


import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootTest
class HotelDemoApplicationTests {
    private RestHighLevelClient client;


    @Autowired
    private IHotelService hotelService;


    @Test
    void testSever(){
//        Map<String, List<String>> filters = service.filters();
//        System.out.println(filters);
    }

    /**
     * 查询所有文档
     */
    @Test
    void testMatchAll() throws IOException {
//        准备request
        SearchRequest request = new SearchRequest("hotel");
//        组织DSL参数
        request.source().query(QueryBuilders.matchAllQuery());
        //发送请求，得到响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        extracted(response);
    }


    /**
     * match查询
     * "match":{
     * "all":"如家"
     * }
     */
    @Test
    void testMatch() throws IOException {
//        准备request
        SearchRequest request = new SearchRequest("hotel");
//        组织DSL参数
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
        //发送请求，得到响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析结果
        extracted(response);
    }

    /**
     * match查询
     * "range":{
     * "price":{
     * "lte":500
     * "gte":200
     * }
     * <p>
     * }
     */
    @Test
    void testRange() throws IOException {
//        准备request
        SearchRequest request = new SearchRequest("hotel");
//        组织DSL参数
        request.source().query(QueryBuilders.rangeQuery("price").gte("200").lte("500"));
        //发送请求，得到响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析结果
        extracted(response);
    }

    /**
     * 精确查询
     * term
     */
    @Test
    void termTest() throws IOException {
//        准备request
        SearchRequest request = new SearchRequest("hotel");
//        组织DSL参数
        request.source().query(QueryBuilders.termQuery("brand", "如家"));
        //发送请求，得到响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析结果
        extracted(response);
    }

    /**
     * #multi_match查询
     */
    @Test
    void multiTest() throws IOException {
//        准备request
        SearchRequest request = new SearchRequest("hotel");
//        组织DSL参数
        request.source().query(QueryBuilders.multiMatchQuery("如家外滩", "brand", "name", "business"));
        //发送请求，得到响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析结果
        extracted(response);
    }

    /**
     * 布尔查询
     * "must": [
     * {
     * "match": {
     * "name": "如家"
     * }
     * }
     * ],
     * "filter": [
     * {
     * "range": {
     * "price": {
     * "gte": 400
     * <p>
     * }
     * }
     * }
     * ]
     */
    @Test
    void testBool() throws IOException {
//        准备request
        SearchRequest request = new SearchRequest("hotel");
//        创建boolQuery对象
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
//        组织DSL参数
        boolQuery.must(QueryBuilders.matchQuery("name", "如家"));
        boolQuery.filter(QueryBuilders.rangeQuery("price").gt("400"));

        request.source().query(boolQuery);
        //发送请求，得到响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析结果
        extracted(response);
    }

    /**
     * 排序，分页，高亮
     */
    @Test
    void testSort() throws IOException {
//           准备request
        SearchRequest request = new SearchRequest("hotel");
//           组织DSL参数
        request.source().query(QueryBuilders.matchQuery("name", "如家"));
//        分页处理
        request.source().from(0).size(20);
//        排序
        request.source().sort("price", SortOrder.DESC);
//        高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));

        //发送请求，得到响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析结果
        extracted(response);
    }

    /**
     * 排序
     * 按照地理位置远近排序
     */
    @Test
    void SortByDistance() throws IOException {
//           准备request
        SearchRequest request = new SearchRequest("hotel");
//           组织DSL参数
        request.source().sort(SortBuilders.geoDistanceSort("location", new GeoPoint("31.21,121.5")).order(SortOrder.DESC).unit(DistanceUnit.KILOMETERS));

        //发送请求，得到响应结果
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //解析结果
        extracted(response);
    }


    /**
     * 聚合查询
     */
    @Test
    void testAggregation() throws IOException {
        //1.准备request
        SearchRequest request = new SearchRequest("hotel");
        //2.1设置size
        request.source().size(0);
        //2.2聚合
        request.source().aggregation(
                AggregationBuilders
                        .terms("brandAgg")
                        .field("brand")
                        .size(10)
        );
        //3.发出请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.1解析结果
        Aggregations aggregations = response.getAggregations();
        Terms brandTerms=aggregations.get("brandAgg");
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        //4.2遍历集合
        for (Terms.Bucket bucket :buckets){
            //4.3获取key(酒店名称)和docCount(酒店数量)
            String key = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();
            System.out.println(key+":"+docCount);
        }


    }

    /**
     * 解析请求返回的response对象
     */
    private static void extracted(SearchResponse response) {
        //解析结果
        SearchHits searchHits = response.getHits();

        long total = searchHits.getTotalHits().value;
        //输出查询文档总数
        System.out.println("一共检索出" + total + "条数据");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
//            获取文档source
            String json = hit.getSourceAsString();
//            反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
//            获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();

            if (!CollectionUtils.isEmpty(highlightFields)) {
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
//                    获取高亮值
                    String name = highlightField.getFragments()[0].string();
//                     覆盖非高亮结果
                    hotelDoc.setName(name);
                }
            }

            System.out.println(hotelDoc);
        }
    }

    /**
     * suggest自动补全
     *
     *
     * */
    @Test
    void testSuggest() throws IOException {
        //1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        //2.准备DSL
        request.source().suggest(new SuggestBuilder().addSuggestion(
                "mySuggestion",
                SuggestBuilders.completionSuggestion("suggestion")
                        .prefix("")
                        .skipDuplicates(true)
                        .size(10)
        ));
        //3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4.解析结果
        Suggest suggest = response.getSuggest();
        CompletionSuggestion mySuggestion = suggest.getSuggestion("mySuggestion");
        List<CompletionSuggestion.Entry.Option> options = mySuggestion.getOptions();
        for (Suggest.Suggestion.Entry.Option option:options){
            String text = option.getText().toString();
            System.out.println(text);
        }
    }
    @Test
    void testAll() throws IOException {
        List<Hotel> list = hotelService.list();
        try {
            FileOutputStream file= new FileOutputStream("D://test.txt");
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(file, "UTF-8");

            for (Hotel hotel :list){
                HotelDoc hotelDoc = new HotelDoc(hotel);
                outputStreamWriter.append(JSON.toJSONString(hotelDoc)+"\n");
            }
            outputStreamWriter.append("\r\n");
            outputStreamWriter.close();
            file.close();

            FileInputStream fileInputStream = new FileInputStream("D://test.txt");
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            StringBuffer stringBuffer = new StringBuffer();
            while (inputStreamReader.ready()){
                stringBuffer.append((char) inputStreamReader.read());
            }
            System.out.println(stringBuffer);
        } catch (IOException e) {
            System.out.print("Exception");
        }




    }
    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://localhost:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

}
