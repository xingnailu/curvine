// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package io.curvine.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.curvine.CurvineFileSystem;
import io.curvine.FilesystemConf;
import io.curvine.worker.oss.OssDataReader;
import io.curvine.worker.sync.CurvineDataWriter;
import io.curvine.worker.sync.DataSyncPipeline;
import io.curvine.worker.util.HeartbeatManager;
import io.curvine.worker.util.TaskProgressReporter;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OSS数据同步运行器
 * 
 * 这个类是独立JVM进程的入口点，负责：
 * 1. 使用JindoData读取OSS/OSS-HDFS数据
 * 2. 使用Curvine LibSDK写入数据到Curvine
 * 3. 管理同步进度和异常处理
 */
public class OssDataSyncRunner {
    private static final Logger LOG = LoggerFactory.getLogger(OssDataSyncRunner.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    
    private String taskId;
    private String sourcePath;
    private String targetPath;
    private Map<String, String> ossConfig;
    private Map<String, String> curvineConfig;
    
    private HeartbeatManager heartbeatManager;
    private TaskProgressReporter progressReporter;
    private DataSyncPipeline syncPipeline;

    public static void main(String[] args) {
        OssDataSyncRunner runner = new OssDataSyncRunner();
        try {
            runner.run(args);
        } catch (Exception e) {
            LOG.error("OSS data sync runner failed", e);
            System.exit(1);
        }
    }

    public void run(String[] args) throws Exception {
        // 解析命令行参数
        parseCommandLineArgs(args);
        
        // 设置JVM关闭钩子
        setupShutdownHook();
        
        // 初始化组件
        initializeComponents();
        
        LOG.info("Starting OSS data sync task: {}", taskId);
        System.out.println("TASK_STARTED"); // Rust侧会监听这个消息
        
        running.set(true);
        
        try {
            // 启动心跳管理器
            heartbeatManager.start();
            
            // 执行数据同步
            executeSyncTask();
            
            LOG.info("OSS data sync task completed successfully: {}", taskId);
            System.out.println("TASK_COMPLETED");
            
        } catch (Exception e) {
            LOG.error("OSS data sync task failed: {}", taskId, e);
            throw e;
        } finally {
            cleanup();
        }
    }

    private void parseCommandLineArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("task-id", true, "Task ID");
        options.addOption("source-path", true, "Source OSS path");
        options.addOption("target-path", true, "Target Curvine path");
        options.addOption("oss-config", true, "OSS configuration JSON");
        options.addOption("curvine-config", true, "Curvine configuration JSON");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        taskId = cmd.getOptionValue("task-id");
        sourcePath = cmd.getOptionValue("source-path");
        targetPath = cmd.getOptionValue("target-path");
        
        String ossConfigJson = cmd.getOptionValue("oss-config");
        String curvineConfigJson = cmd.getOptionValue("curvine-config");

        if (taskId == null || sourcePath == null || targetPath == null || 
            ossConfigJson == null || curvineConfigJson == null) {
            throw new IllegalArgumentException("Missing required command line arguments");
        }

        // 解析JSON配置
        TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {};
        ossConfig = JSON_MAPPER.readValue(ossConfigJson, typeRef);
        curvineConfig = JSON_MAPPER.readValue(curvineConfigJson, typeRef);

        LOG.info("Parsed command line args - Task ID: {}, Source: {}, Target: {}", 
                taskId, sourcePath, targetPath);
    }

    private void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Received shutdown signal, stopping OSS sync task: {}", taskId);
            stop();
            try {
                shutdownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    private void initializeComponents() throws Exception {
        // 初始化心跳管理器
        heartbeatManager = new HeartbeatManager(taskId);
        
        // 初始化进度报告器
        progressReporter = new TaskProgressReporter(taskId);
        
        // 初始化数据同步管道
        syncPipeline = createSyncPipeline();
    }

    private DataSyncPipeline createSyncPipeline() throws Exception {
        // 创建OSS数据读取器
        OssDataReader ossReader = new OssDataReader(sourcePath, ossConfig);
        
        // 创建Curvine数据写入器
        CurvineDataWriter curvineWriter = new CurvineDataWriter(targetPath, curvineConfig);
        
        // 创建同步管道
        DataSyncPipeline pipeline = new DataSyncPipeline(
            ossReader, 
            curvineWriter, 
            progressReporter
        );
        
        return pipeline;
    }

    private void executeSyncTask() throws Exception {
        try {
            // 执行数据同步
            syncPipeline.execute();
        } catch (Exception e) {
            LOG.error("Data sync pipeline failed", e);
            throw e;
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            LOG.info("Stopping OSS data sync task: {}", taskId);
            
            if (syncPipeline != null) {
                syncPipeline.stop();
            }
            
            if (heartbeatManager != null) {
                heartbeatManager.stop();
            }
        }
    }

    private void cleanup() {
        try {
            if (syncPipeline != null) {
                syncPipeline.cleanup();
            }
            
            if (heartbeatManager != null) {
                heartbeatManager.cleanup();
            }
            
            if (progressReporter != null) {
                progressReporter.cleanup();
            }
            
        } catch (Exception e) {
            LOG.warn("Error during cleanup", e);
        } finally {
            shutdownLatch.countDown();
        }
    }
}
