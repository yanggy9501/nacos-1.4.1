/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.consistency.ephemeral.distro.component;

import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.distributed.distro.exception.DistroException;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.combined.DistroHttpCombinedKey;
import com.alibaba.nacos.naming.misc.NamingProxy;

import java.util.ArrayList;
import java.util.List;

/**
 * Distro http agent.
 *
 * @author xiweng.yy
 */
public class DistroHttpAgent implements DistroTransportAgent {

    private final ServerMemberManager memberManager;

    public DistroHttpAgent(ServerMemberManager memberManager) {
        this.memberManager = memberManager;
    }

    @Override
    public boolean syncData(DistroData data, String targetServer) {
        if (!memberManager.hasMember(targetServer)) {
            return true;
        }
        // 获取要同步的数据
        byte[] dataContent = data.getContent();
        // 使用 NamingProxy 进行同步
        return NamingProxy.syncData(dataContent, data.getDistroKey().getTargetServer());
    }

    @Override
    public void syncData(DistroData data, String targetServer, DistroCallback callback) {

    }

    @Override
    public boolean syncVerifyData(DistroData verifyData, String targetServer) {
        if (!memberManager.hasMember(targetServer)) {
            return true;
        }
        // 通过 NamingProxy 同步校验
        NamingProxy.syncCheckSums(verifyData.getContent(), targetServer);
        return true;
    }

    @Override
    public void syncVerifyData(DistroData verifyData, String targetServer, DistroCallback callback) {

    }

    @Override
    public DistroData getData(DistroKey key, String targetServer) {
        try {
            List<String> toUpdateKeys = null;
            if (key instanceof DistroHttpCombinedKey) {
                toUpdateKeys = ((DistroHttpCombinedKey) key).getActualResourceTypes();
            } else {
                toUpdateKeys = new ArrayList<>(1);
                toUpdateKeys.add(key.getResourceKey());
            }
            byte[] queriedData = NamingProxy.getData(toUpdateKeys, key.getTargetServer());
            return new DistroData(key, queriedData);
        } catch (Exception e) {
            throw new DistroException(String.format("Get data from %s failed.", key.getTargetServer()), e);
        }
    }

    @Override
    public DistroData getDatumSnapshot(String targetServer) {
        try {
            //从namingProxy代理获取所有的数据data，从获取的结果result中获取数据bytes；
            byte[] allDatum = NamingProxy.getAllData(targetServer);
            //将数据封装成DistroData
            return new DistroData(new DistroKey("snapshot", KeyBuilder.INSTANCE_LIST_KEY_PREFIX), allDatum);
        } catch (Exception e) {
            throw new DistroException(String.format("Get snapshot from %s failed.", targetServer), e);
        }
    }
}
