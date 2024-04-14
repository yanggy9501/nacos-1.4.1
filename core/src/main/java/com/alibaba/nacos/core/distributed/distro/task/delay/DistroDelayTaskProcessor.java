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

package com.alibaba.nacos.core.distributed.distro.task.delay;

import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.distributed.distro.task.DistroTaskEngineHolder;
import com.alibaba.nacos.core.distributed.distro.task.execute.DistroSyncChangeTask;

/**
 * Distro delay task processor.
 * Distro 延迟任务执行器
 * @author xiweng.yy
 */
public class DistroDelayTaskProcessor implements NacosTaskProcessor {

    private final DistroTaskEngineHolder distroTaskEngineHolder;

    private final DistroComponentHolder distroComponentHolder;

    public DistroDelayTaskProcessor(DistroTaskEngineHolder distroTaskEngineHolder,
            DistroComponentHolder distroComponentHolder) {
        this.distroTaskEngineHolder = distroTaskEngineHolder;
        this.distroComponentHolder = distroComponentHolder;
    }

    @Override
    public boolean process(NacosTask task) {
        if (!(task instanceof DistroDelayTask)) {
            return true;
        }
        // 处理 DistroDelayTask 任务
        DistroDelayTask distroDelayTask = (DistroDelayTask) task;
        DistroKey distroKey = distroDelayTask.getDistroKey();
        // 操作类型时 数据的变更 CHANGE
        if (DataOperation.CHANGE.equals(distroDelayTask.getAction())) {
            // 封装 最终的 DistroSyncChangeTask 任务
            DistroSyncChangeTask syncChangeTask = new DistroSyncChangeTask(distroKey, distroComponentHolder);
            // 提交去执行  下一个入口：com.alibaba.nacos.core.distributed.distro.task.execute.DistroSyncChangeTask.run
            distroTaskEngineHolder.getExecuteWorkersManager().addTask(distroKey, syncChangeTask);
            return true;
        }
        return false;
    }
}
