package com.dyingbleed.akita.source;


/**
 * Created by 李震 on 2017/12/4.
 */
public interface AkitaSource {


    /**
     * 初始化 Source
     */
    public void init();


    /**
     * 从 Source 拉数据
     *
     * @param i 期望拉取数据的条目数
     *
     * @return
     *
     * 数据
     *
     * */
    public void pull(int i, AkitaSourceCallback callback);


    /**
     * 销毁 Source
     * */
    public void destroy();

}
