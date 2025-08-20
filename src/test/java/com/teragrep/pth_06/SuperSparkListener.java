/*
 * Teragrep Archive Datasource (pth_06)
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth_06;

import org.apache.spark.scheduler.*;
import org.apache.spark.util.AccumulatorV2;
import scala.collection.Iterator;

public class SuperSparkListener extends SparkListener {

    public SuperSparkListener() {
        super();
    }

    @Override
    public void onStageCompleted(final SparkListenerStageCompleted stageCompleted) {
        super.onStageCompleted(stageCompleted);

        StageInfo stageInfo = stageCompleted.stageInfo();

        System.out.println("LEL stageInfo.accumulables() " + stageInfo.accumulables());

        System.out.println("LEL stageInfo.details() " + stageInfo.details());

        Iterator<AccumulatorV2<?, ?>> it = stageInfo.taskMetrics().accumulators().iterator();

        while (it.hasNext()) {
            AccumulatorV2<?, ?> next = it.next();
            String key = next.name().get();
            Object value = next.value();
            System.out.printf("LEL key: %s, value: %s%n", key, value);
        }

    }

    @Override
    public void onStageSubmitted(final SparkListenerStageSubmitted stageSubmitted) {
        super.onStageSubmitted(stageSubmitted);
    }

    @Override
    public void onTaskStart(final SparkListenerTaskStart taskStart) {
        super.onTaskStart(taskStart);
    }

    @Override
    public void onTaskGettingResult(final SparkListenerTaskGettingResult taskGettingResult) {
        super.onTaskGettingResult(taskGettingResult);
    }

    @Override
    public void onTaskEnd(final SparkListenerTaskEnd taskEnd) {
        super.onTaskEnd(taskEnd);
    }

    @Override
    public void onJobStart(final SparkListenerJobStart jobStart) {
        super.onJobStart(jobStart);
    }

    @Override
    public void onJobEnd(final SparkListenerJobEnd jobEnd) {
        super.onJobEnd(jobEnd);
    }

    @Override
    public void onEnvironmentUpdate(final SparkListenerEnvironmentUpdate environmentUpdate) {
        super.onEnvironmentUpdate(environmentUpdate);
    }

    @Override
    public void onBlockManagerAdded(final SparkListenerBlockManagerAdded blockManagerAdded) {
        super.onBlockManagerAdded(blockManagerAdded);
    }

    @Override
    public void onBlockManagerRemoved(final SparkListenerBlockManagerRemoved blockManagerRemoved) {
        super.onBlockManagerRemoved(blockManagerRemoved);
    }

    @Override
    public void onUnpersistRDD(final SparkListenerUnpersistRDD unpersistRDD) {
        super.onUnpersistRDD(unpersistRDD);
    }

    @Override
    public void onApplicationStart(final SparkListenerApplicationStart applicationStart) {
        super.onApplicationStart(applicationStart);
    }

    @Override
    public void onApplicationEnd(final SparkListenerApplicationEnd applicationEnd) {
        super.onApplicationEnd(applicationEnd);
    }

    @Override
    public void onExecutorMetricsUpdate(final SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
        super.onExecutorMetricsUpdate(executorMetricsUpdate);
        System.out.println("LEL executorMetricsUpdate: " + executorMetricsUpdate);

        System.out
                .println(
                        "LEL executorMetricsUpdate.accumUpdates().toList() "
                                + executorMetricsUpdate.accumUpdates().toList()
                );
        /*
        System.out
                .println(
                        "LEL executorMetricsUpdate.executorUpdates().toList() XYZ " + executorMetricsUpdate
                                .executorUpdates()
                                .toList()
                                .last()
                                ._2()
                                .getMetricValue("super_metric")
                );
                
         */
    }

    @Override
    public void onStageExecutorMetrics(final SparkListenerStageExecutorMetrics executorMetrics) {
        System.out.println("LEL executorMetrics.executorMetrics(): " + executorMetrics.executorMetrics());

        /*
        System.out
                .println(
                        "LEL executorMetrics.executorMetrics().getMetricValue(\"super_metric\"): "
                                + executorMetrics.executorMetrics().getMetricValue("super_metric")
                );
        
         */
        super.onStageExecutorMetrics(executorMetrics);
    }

    @Override
    public void onExecutorAdded(final SparkListenerExecutorAdded executorAdded) {
        super.onExecutorAdded(executorAdded);
    }

    @Override
    public void onExecutorRemoved(final SparkListenerExecutorRemoved executorRemoved) {
        super.onExecutorRemoved(executorRemoved);
    }

    @Override
    public void onExecutorBlacklisted(final SparkListenerExecutorBlacklisted executorBlacklisted) {
        super.onExecutorBlacklisted(executorBlacklisted);
    }

    @Override
    public void onExecutorExcluded(final SparkListenerExecutorExcluded executorExcluded) {
        super.onExecutorExcluded(executorExcluded);
    }

    @Override
    public void onExecutorBlacklistedForStage(
            final SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage
    ) {
        super.onExecutorBlacklistedForStage(executorBlacklistedForStage);
    }

    @Override
    public void onExecutorExcludedForStage(final SparkListenerExecutorExcludedForStage executorExcludedForStage) {
        super.onExecutorExcludedForStage(executorExcludedForStage);
    }

    @Override
    public void onNodeBlacklistedForStage(final SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
        super.onNodeBlacklistedForStage(nodeBlacklistedForStage);
    }

    @Override
    public void onNodeExcludedForStage(final SparkListenerNodeExcludedForStage nodeExcludedForStage) {
        super.onNodeExcludedForStage(nodeExcludedForStage);
    }

    @Override
    public void onExecutorUnblacklisted(final SparkListenerExecutorUnblacklisted executorUnblacklisted) {
        super.onExecutorUnblacklisted(executorUnblacklisted);
    }

    @Override
    public void onExecutorUnexcluded(final SparkListenerExecutorUnexcluded executorUnexcluded) {
        super.onExecutorUnexcluded(executorUnexcluded);
    }

    @Override
    public void onNodeBlacklisted(final SparkListenerNodeBlacklisted nodeBlacklisted) {
        super.onNodeBlacklisted(nodeBlacklisted);
    }

    @Override
    public void onNodeExcluded(final SparkListenerNodeExcluded nodeExcluded) {
        super.onNodeExcluded(nodeExcluded);
    }

    @Override
    public void onNodeUnblacklisted(final SparkListenerNodeUnblacklisted nodeUnblacklisted) {
        super.onNodeUnblacklisted(nodeUnblacklisted);
    }

    @Override
    public void onNodeUnexcluded(final SparkListenerNodeUnexcluded nodeUnexcluded) {
        super.onNodeUnexcluded(nodeUnexcluded);
    }

    @Override
    public void onUnschedulableTaskSetAdded(final SparkListenerUnschedulableTaskSetAdded unschedulableTaskSetAdded) {
        super.onUnschedulableTaskSetAdded(unschedulableTaskSetAdded);
    }

    @Override
    public void onUnschedulableTaskSetRemoved(
            final SparkListenerUnschedulableTaskSetRemoved unschedulableTaskSetRemoved
    ) {
        super.onUnschedulableTaskSetRemoved(unschedulableTaskSetRemoved);
    }

    @Override
    public void onBlockUpdated(final SparkListenerBlockUpdated blockUpdated) {
        super.onBlockUpdated(blockUpdated);
    }

    @Override
    public void onSpeculativeTaskSubmitted(final SparkListenerSpeculativeTaskSubmitted speculativeTask) {
        super.onSpeculativeTaskSubmitted(speculativeTask);
    }

    @Override
    public void onOtherEvent(final SparkListenerEvent event) {
        super.onOtherEvent(event);
    }

    @Override
    public void onResourceProfileAdded(final SparkListenerResourceProfileAdded event) {
        super.onResourceProfileAdded(event);
    }

}
