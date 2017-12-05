package com.dyingbleed.akita.source;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;

/**
 * Created by 李震 on 2017/12/5.
 */
@FunctionalInterface
public interface AkitaSourceCallback {

    public void processEntry(Entry entry);

}
