package com.dyingbleed.akita.source;

/**
 * Created by 李震 on 2017/12/5.
 */
@FunctionalInterface
public interface AkitaSourceCallback {

    public void processEntry(String key, String value);

}
