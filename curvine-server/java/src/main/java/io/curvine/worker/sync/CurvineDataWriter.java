// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package io.curvine.worker.sync;

import io.curvine.CurvineFileSystem;
import io.curvine.FilesystemConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Curvine数据写入器
 * 
 * 使用Curvine LibSDK将数据写入Curvine文件系统
 */
public class CurvineDataWriter {
    private static final Logger LOG = LoggerFactory.getLogger(CurvineDataWriter.class);
    
    private final String targetPath;
    private final Map<String, String> curvineConfig;
    private final Configuration hadoopConf;
    
    private CurvineFileSystem curvineFS;
    private FSDataOutputStream outputStream;
    private long bytesWritten;
    private boolean isOpen;

    public CurvineDataWriter(String targetPath, Map<String, String> curvineConfig) throws IOException {
        this.targetPath = targetPath;
        this.curvineConfig = curvineConfig;
        this.hadoopConf = createHadoopConfiguration(curvineConfig);
        this.bytesWritten = 0;
        this.isOpen = false;
        
        LOG.info("Created Curvine data writer for path: {}", targetPath);
    }

    /**
     * 打开写入流
     */
    public void open() throws IOException {
        if (isOpen) {
            return;
        }
        
        try {
            LOG.info("Opening Curvine data writer for path: {}", targetPath);
            
            // 创建Curvine文件系统客户端
            curvineFS = new CurvineFileSystem();
            String masterAddr = curvineConfig.get("curvine.master.address");
            if (masterAddr == null) {
                masterAddr = "localhost:29999"; // 默认地址
            }
            
            // 初始化文件系统
            URI uri = URI.create("cv://" + masterAddr);
            curvineFS.initialize(uri, hadoopConf);
            
            // 创建文件
            Path path = new Path(targetPath);
            outputStream = curvineFS.create(path, true); // overwrite=true
            
            bytesWritten = 0;
            isOpen = true;
            
            LOG.info("Successfully opened Curvine data writer for path: {}", targetPath);
            
        } catch (Exception e) {
            LOG.error("Failed to open Curvine data writer: {}", targetPath, e);
            cleanup();
            throw new IOException("Failed to open Curvine data writer: " + targetPath, e);
        }
    }

    /**
     * 写入数据
     */
    public void writeChunk(ByteBuffer buffer) throws IOException {
        if (!isOpen) {
            throw new IOException("Curvine data writer is not open");
        }
        
        if (!buffer.hasRemaining()) {
            return;
        }
        
        try {
            int bytesToWrite = buffer.remaining();
            
            // 将ByteBuffer转换为字节数组
            byte[] data = new byte[bytesToWrite];
            buffer.get(data);
            
            // 写入数据到输出流
            outputStream.write(data);
            
            bytesWritten += bytesToWrite;
            
            LOG.debug("Wrote {} bytes to Curvine, total written: {}", 
                     bytesToWrite, bytesWritten);
            
        } catch (IOException e) {
            LOG.error("Failed to write data to Curvine", e);
            throw e;
        }
    }

    /**
     * 刷新缓冲区
     */
    public void flush() throws IOException {
        if (!isOpen) {
            return;
        }
        
        try {
            outputStream.flush();
            LOG.debug("Flushed Curvine writer");
        } catch (IOException e) {
            LOG.error("Failed to flush Curvine writer", e);
            throw e;
        }
    }

    /**
     * 获取已写入字节数
     */
    public long getBytesWritten() {
        return bytesWritten;
    }

    /**
     * 关闭写入流
     */
    public void close() throws IOException {
        if (!isOpen) {
            return;
        }
        
        try {
            LOG.info("Closing Curvine data writer for path: {}", targetPath);
            
            // 完成写入
            if (outputStream != null) {
                outputStream.flush();
                outputStream.close();
                outputStream = null;
            }
            
            isOpen = false;
            LOG.info("Successfully closed Curvine data writer, total bytes written: {}", bytesWritten);
            
        } catch (Exception e) {
            LOG.error("Error closing Curvine data writer", e);
            throw new IOException("Error closing Curvine data writer", e);
        } finally {
            cleanup();
        }
    }

    private void cleanup() {
        try {
            if (curvineFS != null) {
                curvineFS.close();
                curvineFS = null;
            }
        } catch (Exception e) {
            LOG.warn("Error during Curvine writer cleanup", e);
        }
    }

    /**
     * 创建Hadoop配置
     */
    private Configuration createHadoopConfiguration(Map<String, String> config) {
        Configuration conf = new Configuration();
        
        // 设置基本配置
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            
            // 直接设置到Hadoop配置中
            conf.set(key, value);
        }
        
        // 设置Curvine特定配置
        if (config.containsKey("curvine.replicas")) {
            conf.set("dfs.replication", config.get("curvine.replicas"));
        }
        
        if (config.containsKey("curvine.block.size")) {
            conf.set("dfs.blocksize", config.get("curvine.block.size"));
        }
        
        LOG.info("Created Hadoop configuration for Curvine");
        return conf;
    }

    /**
     * 获取配置信息摘要
     */
    public String getConfigSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("targetPath=").append(targetPath);
        sb.append(", replicas=").append(curvineConfig.get("curvine.replicas"));
        sb.append(", blockSize=").append(curvineConfig.get("curvine.block.size"));
        sb.append(", storageType=").append(curvineConfig.get("curvine.storage.type"));
        return sb.toString();
    }
}
