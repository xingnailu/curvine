// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package io.curvine.worker.oss;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * OSS数据读取器
 * 
 * 使用JindoData SDK读取OSS/OSS-HDFS数据
 */
public class OssDataReader {
    private static final Logger LOG = LoggerFactory.getLogger(OssDataReader.class);
    
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024; // 64KB
    private static final int MAX_BUFFER_SIZE = 8 * 1024 * 1024; // 8MB
    
    private final String sourcePath;
    private final Map<String, String> ossConfig;
    private final Configuration hadoopConf;
    
    private FileSystem fileSystem;
    private FSDataInputStream inputStream;
    private Path ossPath;
    private long fileSize;
    private long bytesRead;
    private boolean isOpen;
    
    private final ByteBuffer readBuffer;

    public OssDataReader(String sourcePath, Map<String, String> ossConfig) throws IOException {
        this.sourcePath = sourcePath;
        this.ossConfig = ossConfig;
        this.hadoopConf = createHadoopConfiguration(ossConfig);
        
        // 创建读取缓冲区
        int bufferSize = getConfiguredBufferSize();
        this.readBuffer = ByteBuffer.allocateDirect(bufferSize);
        
        LOG.info("Created OSS data reader for path: {}, buffer size: {}", sourcePath, bufferSize);
    }

    /**
     * 打开数据流
     */
    public void open() throws IOException {
        if (isOpen) {
            return;
        }
        
        try {
            LOG.info("Opening OSS data stream for path: {}", sourcePath);
            
            // 解析OSS路径
            ossPath = new Path(sourcePath);
            
            // 创建文件系统
            fileSystem = FileSystem.get(URI.create(sourcePath), hadoopConf);
            
            // 获取文件大小
            fileSize = fileSystem.getFileStatus(ossPath).getLen();
            LOG.info("OSS file size: {} bytes", fileSize);
            
            // 打开输入流
            inputStream = fileSystem.open(ossPath);
            
            bytesRead = 0;
            isOpen = true;
            
            LOG.info("Successfully opened OSS data stream");
            
        } catch (Exception e) {
            LOG.error("Failed to open OSS data stream: {}", sourcePath, e);
            cleanup();
            throw new IOException("Failed to open OSS data stream: " + sourcePath, e);
        }
    }

    /**
     * 读取数据到缓冲区
     * 
     * @return 读取的字节数，-1表示EOF
     */
    public int readChunk() throws IOException {
        if (!isOpen) {
            throw new IOException("OSS data reader is not open");
        }
        
        if (bytesRead >= fileSize) {
            return -1; // EOF
        }
        
        try {
            readBuffer.clear();
            
            int totalRead = 0;
            while (readBuffer.hasRemaining() && bytesRead + totalRead < fileSize) {
                byte[] tempBuffer = new byte[Math.min(readBuffer.remaining(), 8192)];
                int bytesReadThisTime = inputStream.read(tempBuffer);
                
                if (bytesReadThisTime == -1) {
                    break; // EOF
                }
                
                readBuffer.put(tempBuffer, 0, bytesReadThisTime);
                totalRead += bytesReadThisTime;
            }
            
            readBuffer.flip();
            bytesRead += totalRead;
            
            LOG.debug("Read {} bytes from OSS, total read: {}/{}", 
                     totalRead, bytesRead, fileSize);
            
            return totalRead;
            
        } catch (IOException e) {
            LOG.error("Failed to read data from OSS", e);
            throw e;
        }
    }

    /**
     * 获取读取缓冲区
     */
    public ByteBuffer getReadBuffer() {
        return readBuffer.asReadOnlyBuffer();
    }

    /**
     * 获取文件总大小
     */
    public long getFileSize() {
        return fileSize;
    }

    /**
     * 获取已读取字节数
     */
    public long getBytesRead() {
        return bytesRead;
    }

    /**
     * 获取读取进度 (0.0 - 1.0)
     */
    public double getProgress() {
        if (fileSize == 0) {
            return 1.0;
        }
        return (double) bytesRead / fileSize;
    }

    /**
     * 检查是否已读取完成
     */
    public boolean isEOF() {
        return bytesRead >= fileSize;
    }

    /**
     * 关闭数据流
     */
    public void close() throws IOException {
        isOpen = false;
        cleanup();
        LOG.info("Closed OSS data reader for path: {}", sourcePath);
    }

    private void cleanup() {
        try {
            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        } catch (IOException e) {
            LOG.warn("Error closing OSS input stream", e);
        }
        
        try {
            if (fileSystem != null) {
                fileSystem.close();
                fileSystem = null;
            }
        } catch (IOException e) {
            LOG.warn("Error closing OSS file system", e);
        }
    }

    /**
     * 创建Hadoop配置
     */
    private Configuration createHadoopConfiguration(Map<String, String> ossConfig) {
        Configuration conf = new Configuration();
        
        // 基础配置
        conf.set("fs.defaultFS", "oss://");
        
        // OSS配置
        String endpoint = ossConfig.get("fs.oss.endpoint");
        String accessKeyId = ossConfig.get("fs.oss.accessKeyId");
        String accessKeySecret = ossConfig.get("fs.oss.accessKeySecret");
        String bucket = ossConfig.get("fs.oss.bucket");
        
        if (endpoint == null || accessKeyId == null || accessKeySecret == null) {
            throw new IllegalArgumentException("Missing required OSS configuration");
        }
        
        conf.set("fs.oss.endpoint", endpoint);
        conf.set("fs.oss.accessKeyId", accessKeyId);
        conf.set("fs.oss.accessKeySecret", accessKeySecret);
        
        if (bucket != null) {
            conf.set("fs.oss.bucket", bucket);
        }
        
        // 判断是否为OSS-HDFS
        boolean isOssHdfs = endpoint.contains("oss-dls");
        if (isOssHdfs) {
            LOG.info("Detected OSS-HDFS endpoint, attempting to configure JindoData");
            
            // 尝试使用JindoData，如果不可用则降级到标准实现
            if (isJindoDataAvailable()) {
                LOG.info("JindoData is available, using JindoData implementation");
                // OSS-HDFS JindoData配置
                conf.set("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
                conf.set("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.OSS");
                
                // JindoData特定配置
                conf.set("fs.jindo.oss.endpoint", endpoint);
                conf.set("fs.jindo.oss.access.key", accessKeyId);
                conf.set("fs.jindo.oss.access.secret", accessKeySecret);
                
                // JindoData优化配置
                conf.setBoolean("fs.oss.connection.ssl.enabled", endpoint.startsWith("https://"));
                conf.setInt("fs.oss.connection.timeout", 300000); // 5分钟
                conf.setInt("fs.oss.socket.timeout", 300000); // 5分钟
                conf.setInt("fs.oss.connection.maximum", 1024);
            } else {
                LOG.warn("JindoData not available, falling back to standard OSS implementation for OSS-HDFS");
                configureStandardOss(conf, endpoint, accessKeyId, accessKeySecret);
            }
            
        } else {
            LOG.info("Detected standard OSS endpoint, using Hadoop OSS connector");
            configureStandardOss(conf, endpoint, accessKeyId, accessKeySecret);
        }
        
        // 通用性能优化配置
        conf.setInt("fs.oss.multipart.upload.threshold", 100 * 1024 * 1024); // 100MB
        conf.setInt("fs.oss.multipart.upload.part.size", 10 * 1024 * 1024); // 10MB
        conf.setInt("fs.oss.max.connections", 100);
        conf.setBoolean("fs.oss.fast.upload", true);
        
        // 添加其他自定义配置
        for (Map.Entry<String, String> entry : ossConfig.entrySet()) {
            if (!entry.getKey().startsWith("curvine.")) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }
        
        LOG.info("Created Hadoop configuration for OSS access, isOssHdfs: {}", isOssHdfs);
        return conf;
    }

    /**
     * 获取配置的缓冲区大小
     */
    private int getConfiguredBufferSize() {
        String bufferSizeStr = ossConfig.get("fs.oss.buffer.size");
        if (bufferSizeStr != null) {
            try {
                int bufferSize = Integer.parseInt(bufferSizeStr);
                return Math.max(DEFAULT_BUFFER_SIZE, Math.min(bufferSize, MAX_BUFFER_SIZE));
            } catch (NumberFormatException e) {
                LOG.warn("Invalid buffer size configuration: {}, using default", bufferSizeStr);
            }
        }
        return DEFAULT_BUFFER_SIZE;
    }

    /**
     * 获取OSS配置信息摘要（用于日志，隐藏敏感信息）
     */
    public String getConfigSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("endpoint=").append(ossConfig.get("fs.oss.endpoint"));
        sb.append(", bucket=").append(ossConfig.get("fs.oss.bucket"));
        sb.append(", accessKeyId=").append(maskSensitiveInfo(ossConfig.get("fs.oss.accessKeyId")));
        return sb.toString();
    }

    private String maskSensitiveInfo(String info) {
        if (info == null || info.length() <= 6) {
            return "***";
        }
        return info.substring(0, 3) + "***" + info.substring(info.length() - 3);
    }

    /**
     * 检查JindoData是否可用
     */
    private boolean isJindoDataAvailable() {
        try {
            Class.forName("com.aliyun.jindodata.oss.JindoOssFileSystem");
            LOG.info("JindoData classes found in classpath");
            return true;
        } catch (ClassNotFoundException e) {
            LOG.warn("JindoData classes not found in classpath: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 配置标准OSS连接器
     */
    private void configureStandardOss(Configuration conf, String endpoint, String accessKeyId, String accessKeySecret) {
        LOG.info("Configuring standard Hadoop OSS connector");
        
        // 标准Hadoop OSS配置
        conf.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
        conf.set("fs.AbstractFileSystem.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSS");
        
        // OSS连接配置
        conf.set("fs.oss.endpoint", endpoint);
        conf.set("fs.oss.accessKeyId", accessKeyId);
        conf.set("fs.oss.accessKeySecret", accessKeySecret);
        
        // 连接优化配置
        conf.setBoolean("fs.oss.connection.ssl.enabled", endpoint.startsWith("https://"));
        conf.setInt("fs.oss.connection.timeout", 30000); // 30秒
        conf.setInt("fs.oss.socket.timeout", 30000); // 30秒
        conf.setInt("fs.oss.connection.maximum", 64);
        
        // 上传配置
        conf.setBoolean("fs.oss.multipart.upload", true);
        conf.setLong("fs.oss.multipart.upload.size", 5 * 1024 * 1024L); // 5MB
        conf.setInt("fs.oss.multipart.upload.threads", 5);
        
        LOG.info("Standard OSS connector configured successfully");
    }
}
