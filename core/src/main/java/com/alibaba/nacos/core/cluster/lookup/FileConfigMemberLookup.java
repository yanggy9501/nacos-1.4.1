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

package com.alibaba.nacos.core.cluster.lookup;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.cluster.AbstractMemberLookup;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.MemberUtil;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.file.FileChangeEvent;
import com.alibaba.nacos.sys.file.FileWatcher;
import com.alibaba.nacos.sys.file.WatchFileCenter;
import com.alibaba.nacos.core.utils.Loggers;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Cluster.conf file managed cluster member node addressing pattern.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
// 基于文件配置的集群寻址模式（配置文件中配置集群所有服务地址）
public class FileConfigMemberLookup extends AbstractMemberLookup {

    private FileWatcher watcher = new FileWatcher() {
        @Override
        public void onChange(FileChangeEvent event) {
            readClusterConfFromDisk();
        }

        @Override
        public boolean interest(String context) {
            return StringUtils.contains(context, "cluster.conf");
        }
    };

    /**
     * 启动
     *
     * @throws NacosException
     */
    @Override
    public void start() throws NacosException {
        if (start.compareAndSet(false, true)) {
            // 从磁盘读取 cluster.conf 配置文件获取集群节点信息
            readClusterConfFromDisk();
            // Use the inotify mechanism to monitor file changes and automatically
            // trigger the reading of cluster.conf
            try {
                // 监听文件的变更
                WatchFileCenter.registerWatcher(EnvUtil.getConfPath(), watcher);
            } catch (Throwable e) {
                Loggers.CLUSTER.error("An exception occurred in the launch file monitor : {}", e.getMessage());
            }
        }
    }

    @Override
    public void destroy() throws NacosException {
        WatchFileCenter.deregisterWatcher(EnvUtil.getConfPath(), watcher);
    }

    private void readClusterConfFromDisk() {
        Collection<Member> tmpMembers = new ArrayList<>();
        try {
            // 读取配置文件 获取集群节点信息
            List<String> tmp = EnvUtil.readClusterConf();
            // 将每个节点配置转换成 Member 对象
            tmpMembers = MemberUtil.readServerConf(tmp);
        } catch (Throwable e) {
            Loggers.CLUSTER
                .error("nacos-XXXX [serverlist] failed to get serverlist from disk!, error : {}", e.getMessage());
        }
        // 寻址之后的一些操作
        afterLookup(tmpMembers);
    }
}
