// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package io.curvine.worker.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务进度报告器
 * 
 * 报告数据同步进度给Rust侧
 */
public class TaskProgressReporter {
    private static final Logger LOG = LoggerFactory.getLogger(TaskProgressReporter.class);
    
    private final String taskId;
    private long lastReportedBytes = 0;
    private double lastReportedProgress = 0.0;

    public TaskProgressReporter(String taskId) {
        this.taskId = taskId;
    }

    /**
     * 报告进度
     * 
     * @param processedBytes 已处理的字节数
     * @param totalBytes 总字节数
     * @param progress 进度百分比 (0.0 - 1.0)
     */
    public void reportProgress(long processedBytes, long totalBytes, double progress) {
        try {
            // 只在进度有明显变化时才报告，避免过于频繁
            if (shouldReportProgress(processedBytes, progress)) {
                // 向stdout输出进度消息，Rust侧会解析这个消息
                String progressMessage = String.format(
                    "PROGRESS: taskId=%s, processedBytes=%d, totalBytes=%d, progress=%.2f",
                    taskId, processedBytes, totalBytes, progress
                );
                
                System.out.println(progressMessage);
                System.out.flush();
                
                lastReportedBytes = processedBytes;
                lastReportedProgress = progress;
                
                LOG.info("Reported progress for task {}: {}/{} bytes ({:.1f}%)",
                        taskId, processedBytes, totalBytes, progress * 100);
            }
            
        } catch (Exception e) {
            LOG.error("Failed to report progress for task: {}", taskId, e);
        }
    }

    /**
     * 判断是否应该报告进度
     */
    private boolean shouldReportProgress(long processedBytes, double progress) {
        // 进度变化超过1%或字节数变化超过1MB时报告
        long bytesChange = processedBytes - lastReportedBytes;
        double progressChange = Math.abs(progress - lastReportedProgress);
        
        return bytesChange >= 1024 * 1024 || progressChange >= 0.01;
    }

    /**
     * 报告任务开始
     */
    public void reportTaskStarted(long totalBytes) {
        try {
            String message = String.format("TASK_STARTED: taskId=%s, totalBytes=%d", taskId, totalBytes);
            System.out.println(message);
            System.out.flush();
            
            LOG.info("Reported task started: {}, total bytes: {}", taskId, totalBytes);
            
        } catch (Exception e) {
            LOG.error("Failed to report task started for task: {}", taskId, e);
        }
    }

    /**
     * 报告任务完成
     */
    public void reportTaskCompleted(long processedBytes, long durationMs) {
        try {
            String message = String.format(
                "TASK_COMPLETED: taskId=%s, processedBytes=%d, durationMs=%d",
                taskId, processedBytes, durationMs
            );
            
            System.out.println(message);
            System.out.flush();
            
            LOG.info("Reported task completed: {}, processed bytes: {}, duration: {} ms",
                    taskId, processedBytes, durationMs);
            
        } catch (Exception e) {
            LOG.error("Failed to report task completed for task: {}", taskId, e);
        }
    }

    /**
     * 报告任务失败
     */
    public void reportTaskFailed(String errorMessage) {
        try {
            String message = String.format(
                "TASK_FAILED: taskId=%s, error=%s",
                taskId, errorMessage.replace("\n", "\\n")
            );
            
            System.err.println(message);
            System.err.flush();
            
            LOG.error("Reported task failed: {}, error: {}", taskId, errorMessage);
            
        } catch (Exception e) {
            LOG.error("Failed to report task failed for task: {}", taskId, e);
        }
    }

    /**
     * 清理资源
     */
    public void cleanup() {
        // 当前实现不需要清理资源
        LOG.debug("Task progress reporter cleanup completed for task: {}", taskId);
    }
}
