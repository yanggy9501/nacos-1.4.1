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

package com.alibaba.nacos.core.distributed.distro.task.verify;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.List;

/**
 * Distro verify task.
 *
 * @author xiweng.yy
 */
public class DistroVerifyTask implements Runnable {

    private final ServerMemberManager serverMemberManager;

    private final DistroComponentHolder distroComponentHolder;

    public DistroVerifyTask(ServerMemberManager serverMemberManager, DistroComponentHolder distroComponentHolder) {
        this.serverMemberManager = serverMemberManager;
        this.distroComponentHolder = distroComponentHolder;
    }

    /**
     * 分区数据定时同步校验
     */
    @Override
    public void run() {
        try {
            // 获取集群其他节点
            List<Member> targetServer = serverMemberManager.allMembersWithoutSelf();
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("server list is: {}", targetServer);
            }
            // 所有数据均向其他节点进行同步检查
            for (String each : distroComponentHolder.getDataStorageTypes()) {
                verifyForDataStorage(each, targetServer);
            }
        } catch (Exception e) {
            Loggers.DISTRO.error("[DISTRO-FAILED] verify task failed.", e);
        }
    }

    private void verifyForDataStorage(String type, List<Member> targetServer) {
        // 封装校验信息
        DistroData distroData = distroComponentHolder.findDataStorage(type).getVerifyData();
        if (null == distroData) {
            return;
        }
        // 操作类型是 VERIFY 校验
        distroData.setType(DataOperation.VERIFY);
        for (Member member : targetServer) {
            try {
                // 同步检查数据 发送校验信息
                distroComponentHolder.findTransportAgent(type).syncVerifyData(distroData, member.getAddress());
            } catch (Exception e) {
                Loggers.DISTRO.error(String
                        .format("[DISTRO-FAILED] verify data for type %s to %s failed.", type, member.getAddress()), e);
            }
        }
    }
}
