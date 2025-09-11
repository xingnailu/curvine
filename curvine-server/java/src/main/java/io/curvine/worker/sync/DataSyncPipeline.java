// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package io.curvine.worker.sync;

import io.curvine.worker.oss.OssDataReader;
import io.curvine.worker.util.TaskProgressReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 数据同步管道
 * 
 * 协调OSS数据读取和Curvine数据写入
 */
public class DataSyncPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(DataSyncPipeline.class);
    
    private static final long PROGRESS_REPORT_INTERVAL_MS = 10_000; // 10秒
    private static final long FLUSH_INTERVAL_MS = 30_000; // 30秒
    
    private final OssDataReader reader;
    private final CurvineDataWriter writer;
    private final TaskProgressReporter progressReporter;
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    private long totalBytes = 0;
    private long processedBytes = 0;
    private long lastProgressReportTime = 0;
    private long lastFlushTime = 0;

    public DataSyncPipeline(OssDataReader reader, CurvineDataWriter writer, 
                           TaskProgressReporter progressReporter) {
        this.reader = reader;
        this.writer = writer;
        this.progressReporter = progressReporter;
    }

    /**
     * 执行数据同步
     */
    public void execute() throws IOException {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Data sync pipeline is already running");
        }
        
        LOG.info("Starting data sync pipeline");
        long startTime = System.currentTimeMillis();
        
        try {
            // 打开读取器和写入器
            reader.open();
            writer.open();
            
            totalBytes = reader.getFileSize();
            LOG.info("Starting sync for {} bytes", totalBytes);
            
            // 执行数据传输
            performDataTransfer();
            
            // 完成写入
            writer.flush();
            
            long duration = System.currentTimeMillis() - startTime;
            double throughputMBps = processedBytes / 1024.0 / 1024.0 / (duration / 1000.0);
            
            LOG.info("Data sync completed successfully. Processed {} bytes in {} ms, throughput: {:.2f} MB/s",
                    processedBytes, duration, throughputMBps);
            
            // 报告完成状态
            progressReporter.reportProgress(processedBytes, totalBytes, 1.0);
            
        } catch (Exception e) {
            LOG.error("Data sync pipeline failed", e);
            throw e;
        } finally {
            cleanup();
        }
    }

    /**
     * 执行数据传输
     */
    private void performDataTransfer() throws IOException {
        lastProgressReportTime = System.currentTimeMillis();
        lastFlushTime = System.currentTimeMillis();
        
        while (running.get() && !reader.isEOF()) {
            // 检查是否需要停止
            if (!running.get()) {
                LOG.info("Data sync pipeline stopped by request");
                break;
            }
            
            // 从OSS读取数据块
            int bytesRead = reader.readChunk();
            if (bytesRead <= 0) {
                break; // EOF或无数据
            }
            
            // 获取读取的数据
            ByteBuffer buffer = reader.getReadBuffer();
            
            // 写入到Curvine
            writer.writeChunk(buffer);
            
            processedBytes += bytesRead;
            
            // 定期报告进度
            reportProgressIfNeeded();
            
            // 定期刷新写入缓冲区
            flushIfNeeded();
        }
        
        LOG.info("Data transfer completed, processed {} bytes", processedBytes);
    }

    /**
     * 如果需要则报告进度
     */
    private void reportProgressIfNeeded() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastProgressReportTime >= PROGRESS_REPORT_INTERVAL_MS) {
            double progress = totalBytes > 0 ? (double) processedBytes / totalBytes : 0.0;
            progressReporter.reportProgress(processedBytes, totalBytes, progress);
            lastProgressReportTime = currentTime;
            
            LOG.info("Sync progress: {}/{} bytes ({:.1f}%)", 
                    processedBytes, totalBytes, progress * 100);
        }
    }

    /**
     * 如果需要则刷新缓冲区
     */
    private void flushIfNeeded() throws IOException {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastFlushTime >= FLUSH_INTERVAL_MS) {
            writer.flush();
            lastFlushTime = currentTime;
            LOG.debug("Flushed writer buffer");
        }
    }

    /**
     * 停止数据同步
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            LOG.info("Stopping data sync pipeline");
        }
    }

    /**
     * 清理资源
     */
    public void cleanup() {
        running.set(false);
        
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing writer", e);
        }
        
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing reader", e);
        }
        
        LOG.info("Data sync pipeline cleanup completed");
    }

    /**
     * 获取同步统计信息
     */
    public SyncStats getStats() {
        return new SyncStats(
            totalBytes,
            processedBytes,
            totalBytes > 0 ? (double) processedBytes / totalBytes : 0.0,
            running.get()
        );
    }

    /**
     * 同步统计信息
     */
    public static class SyncStats {
        private final long totalBytes;
        private final long processedBytes;
        private final double progress;
        private final boolean isRunning;

        public SyncStats(long totalBytes, long processedBytes, double progress, boolean isRunning) {
            this.totalBytes = totalBytes;
            this.processedBytes = processedBytes;
            this.progress = progress;
            this.isRunning = isRunning;
        }

        public long getTotalBytes() {
            return totalBytes;
        }

        public long getProcessedBytes() {
            return processedBytes;
        }

        public double getProgress() {
            return progress;
        }

        public boolean isRunning() {
            return isRunning;
        }

        @Override
        public String toString() {
            return String.format("SyncStats{totalBytes=%d, processedBytes=%d, progress=%.2f%%, isRunning=%s}",
                    totalBytes, processedBytes, progress * 100, isRunning);
        }
    }
}
