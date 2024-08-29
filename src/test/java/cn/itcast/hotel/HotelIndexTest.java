package cn.itcast.hotel;


import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.IOException;

import static cn.itcast.hotel.CONSTANTS.HotelConstants.MAPPING_TEMPLATE;

//初始化客户端
public class HotelIndexTest {
    private RestHighLevelClient client;

    @Test
    void testInit(){
        System.out.println(client);
    }
//创建索引
    @Test
    void testHotelIndex() throws IOException {
//        1.创建索引库对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
//        2.准备请求的参数：DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);

//        3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }
//    删除索引库
    @Test
    void testHotelDelete() throws IOException {
//        1.创建索引库对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
//        2.发送请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }
    //判断索引存在
    @Test
    void testExistHotelIndex() throws IOException {
//        1.创建索引库对象
        GetIndexRequest request = new GetIndexRequest("hotel");
//        2.发送请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
//        3、输出判断结果
        System.out.println(exists?"索引存在":"索引不存在");
    }




    @BeforeEach
    void setUp() {
        this.client=new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://localhost:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }
}
