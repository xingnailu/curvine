// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package io.curvine.worker.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 心跳管理器
 * 
 * 定期向Rust侧发送心跳信号，证明Java进程仍在运行
 */
public class HeartbeatManager {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatManager.class);
    
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000; // 30秒
    
    private final String taskId;
    private final long heartbeatIntervalMs;
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    private ScheduledExecutorService scheduler;

    public HeartbeatManager(String taskId) {
        this(taskId, DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    public HeartbeatManager(String taskId, long heartbeatIntervalMs) {
        this.taskId = taskId;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    /**
     * 启动心跳管理器
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            LOG.warn("Heartbeat manager is already running for task: {}", taskId);
            return;
        }
        
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "heartbeat-" + taskId);
            thread.setDaemon(true);
            return thread;
        });
        
        // 立即发送一次心跳，然后定期发送
        scheduler.scheduleAtFixedRate(
            this::sendHeartbeat,
            0, // 立即开始
            heartbeatIntervalMs,
            TimeUnit.MILLISECONDS
        );
        
        LOG.info("Started heartbeat manager for task: {} with interval: {} ms", 
                taskId, heartbeatIntervalMs);
    }

    /**
     * 停止心跳管理器
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        
        LOG.info("Stopped heartbeat manager for task: {}", taskId);
    }

    /**
     * 发送心跳信号
     */
    private void sendHeartbeat() {
        if (!running.get()) {
            return;
        }
        
        try {
            // 向stdout输出心跳消息，Rust侧会监听这个消息
            System.out.println("HEARTBEAT");
            System.out.flush();
            
            LOG.debug("Sent heartbeat for task: {}", taskId);
            
        } catch (Exception e) {
            LOG.error("Failed to send heartbeat for task: {}", taskId, e);
        }
    }

    /**
     * 清理资源
     */
    public void cleanup() {
        stop();
    }

    /**
     * 检查心跳管理器是否正在运行
     */
    public boolean isRunning() {
        return running.get();
    }
}
