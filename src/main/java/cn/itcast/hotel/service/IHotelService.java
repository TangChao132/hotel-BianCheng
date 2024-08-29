package cn.itcast.hotel.service;

import cn.itcast.hotel.pojo.dto.RequestParams;
import cn.itcast.hotel.pojo.enity.Hotel;
import cn.itcast.hotel.pojo.vo.PageResult;
import com.baomidou.mybatisplus.extension.service.IService;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

public interface IHotelService extends IService<Hotel> {
    PageResult search(RequestParams params);
    Map<String, List<String>> filters(RequestParams Params);

    List<String> getSuggestion(String prefix);
    void deleteById(Long id);

    void insertById(Long id);

}
