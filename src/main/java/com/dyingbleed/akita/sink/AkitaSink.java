package com.dyingbleed.akita.sink;

import java.util.List;

/**
 * Created by 李震 on 2017/12/4.
 */
public interface AkitaSink {

    /**
     * 初始化 Sink
     * */
    public void init();


    /**
     * 向 Sink 推数据
     *
     * @param key
     * @param value
     *
     * */
    public void push(String key, String value);


    /**
     * 销毁 Sink
     * */
    public void destroy();

}
