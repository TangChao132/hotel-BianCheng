package cn.itcast.hotel.pojo.vo;


import lombok.Data;

import java.util.List;
@Data
public class PageResult {
    private List<HotelDoc> hotels;
    private Long total;

    public PageResult() {
    }

    public PageResult(List<HotelDoc> hotels, Long total) {
        this.hotels = hotels;
        this.total = total;
    }
}
