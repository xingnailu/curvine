// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.curvine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Java端的MountValue类，对应Rust中的MountValue结构
 * 包含挂载信息和对应的UFS文件系统实例
 */
public class MountValue {
    public static final Logger LOGGER = LoggerFactory.getLogger(MountValue.class);
    
    private final MountInfo info;
    private final FileSystem ufs;
    private final String mountId;

    /**
     * 创建MountValue实例
     */
    public MountValue(MountInfo info) throws IOException {
        this.info = info;
        this.mountId = String.valueOf(info.getMountId());
        
        // 根据UFS路径创建对应的文件系统
        this.ufs = createUfsFileSystem(info);
        
        LOGGER.debug("创建MountValue: cvPath={}, ufsPath={}, mountId={}", 
            info.getCvPath(), info.getUfsPath(), mountId);
    }

    /**
     * 根据MountInfo创建UFS文件系统
     */
    private FileSystem createUfsFileSystem(MountInfo info) throws IOException {
        String ufsPath = info.getUfsPath();
        LOGGER.info("开始创建UFS文件系统: {}", ufsPath);
        
        try {
            URI ufsUri = URI.create(ufsPath);
            Configuration conf = new Configuration();
            
            // 设置UFS配置
            LOGGER.debug("应用UFS配置属性:");
            for (Map.Entry<String, String> entry : info.getProperties().entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
                LOGGER.debug("  {} = {}", entry.getKey(), entry.getValue());
            }
            
            // 根据scheme选择合适的文件系统实现
            String scheme = ufsUri.getScheme();
            LOGGER.info("UFS协议: {}, 是否启用OSS-HDFS: {}", scheme, info.isOssHdfsEnabled());
            
            if ("oss".equals(scheme)) {
                // OSS文件系统配置（包括标准OSS和OSS-HDFS）
                configureOssFileSystem(conf, info.getProperties());
                
                // 记录将要使用的FileSystem实现类
                String fsImpl = conf.get("fs.oss.impl", "未设置");
                LOGGER.info("OSS FileSystem实现类: {}", fsImpl);
                
                // 预检查FileSystem实现类是否可用
                if (fsImpl != null && !"未设置".equals(fsImpl)) {
                    checkFileSystemClassAvailability(fsImpl);
                }
            } else if ("s3".equals(scheme)) {
                // S3文件系统配置
                configureS3FileSystem(conf, info.getProperties());
                LOGGER.info("S3 FileSystem配置完成");
            }
            
            LOGGER.info("调用FileSystem.get()创建文件系统实例");
            FileSystem fs = FileSystem.get(ufsUri, conf);
            LOGGER.info("成功创建UFS文件系统: {} -> {}", ufsPath, fs.getClass().getName());
            
            return fs;
            
        } catch (Exception e) {
            String errorMsg = String.format("创建UFS文件系统失败 - 路径: %s, 错误: %s", 
                                           ufsPath, e.getMessage());
            LOGGER.error(errorMsg, e);
            
            // 打印详细的错误信息
            LOGGER.error("异常详情:");
            LOGGER.error("  - UFS路径: {}", ufsPath);
            LOGGER.error("  - 挂载信息: {}", info);
            LOGGER.error("  - 异常类型: {}", e.getClass().getSimpleName());
            LOGGER.error("  - 异常消息: {}", e.getMessage());
            
            if (e.getCause() != null) {
                LOGGER.error("  - 根本原因: {} - {}", e.getCause().getClass().getSimpleName(), 
                           e.getCause().getMessage());
            }
            
            // 抛出包含原始异常的IOException
            throw new IOException(errorMsg, e);
        }
    }

    /**
     * 检查FileSystem实现类是否可用
     */
    private void checkFileSystemClassAvailability(String className) {
        try {
            LOGGER.debug("检查FileSystem实现类是否可用: {}", className);
            Class<?> fsClass = Class.forName(className);
            LOGGER.info("✅ FileSystem实现类检查通过: {} (来源: {})", 
                       className, fsClass.getProtectionDomain().getCodeSource().getLocation());
        } catch (ClassNotFoundException e) {
            LOGGER.warn("⚠️ FileSystem实现类未找到: {}", className);
            LOGGER.warn("请确保相关JAR包在classpath中:");
            if (className.contains("jindodata")) {
                LOGGER.warn("  - 需要 aliyun-sdk-jindodata JAR (Jindo SDK)");
                LOGGER.warn("  - 建议版本: 6.5.1 或更高版本");
                LOGGER.warn("  - 部署位置: $HADOOP_HOME/share/hadoop/tools/lib/ 或应用classpath");
            } else if (className.contains("aliyun.oss")) {
                LOGGER.warn("  - 需要 hadoop-aliyun JAR");
                LOGGER.warn("  - 建议版本: 与Hadoop版本匹配");
            }
            LOGGER.warn("当前classpath: {}", System.getProperty("java.class.path"));
        } catch (Exception e) {
            LOGGER.warn("FileSystem实现类检查异常: {} - {}", className, e.getMessage());
        }
    }

    /**
     * 配置OSS文件系统
     */
    private void configureOssFileSystem(Configuration conf, Map<String, String> properties) {
        String endpointUrl = properties.get("oss.endpoint_url");
        if (endpointUrl != null) {
            conf.set("fs.oss.endpoint", endpointUrl);
        }
        
        String accessKey = properties.get("oss.credentials.access");
        if (accessKey != null) {
            conf.set("fs.oss.accessKeyId", accessKey);
        }
        
        String secretKey = properties.get("oss.credentials.secret");
        if (secretKey != null) {
            conf.set("fs.oss.accessKeySecret", secretKey);
        }
        
        // 根据oss-hdfs-enable参数设置文件系统实现类
        if (info.isOssHdfsEnabled()) {
            conf.set("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
            LOGGER.debug("配置OSS-HDFS文件系统实现：JindoOssFileSystem");
        } else {
            conf.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
            LOGGER.debug("配置标准OSS文件系统实现：AliyunOSSFileSystem");
        }
        
        LOGGER.debug("配置OSS文件系统: endpoint={}, accessKey存在={}", 
            endpointUrl, accessKey != null);
    }

    /**
     * 配置S3文件系统
     */
    private void configureS3FileSystem(Configuration conf, Map<String, String> properties) {
        String endpointUrl = properties.get("s3.endpoint_url");
        if (endpointUrl != null) {
            conf.set("fs.s3a.endpoint", endpointUrl);
        }
        
        String accessKey = properties.get("s3.credentials.access");
        if (accessKey != null) {
            conf.set("fs.s3a.access.key", accessKey);
        }
        
        String secretKey = properties.get("s3.credentials.secret");
        if (secretKey != null) {
            conf.set("fs.s3a.secret.key", secretKey);
        }
        
        // 设置S3文件系统实现类
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        
        LOGGER.debug("配置S3文件系统: endpoint={}, accessKey存在={}", 
            endpointUrl, accessKey != null);
    }

    /**
     * 获取UFS路径，根据CV路径计算对应的UFS路径
     */
    public String getUfsPath(String cvPath) {
        return info.getUfsPath(cvPath);
    }

    /**
     * 获取挂载ID
     */
    public String getMountId() {
        return mountId;
    }

    /**
     * 检查是否是OSS-HDFS协议（通过mount参数判断）
     */
    public boolean isOssHdfsEnabled() {
        return info.isOssHdfsEnabled();
    }

    /**
     * 检查是否是OSS协议（包括OSS和OSS-HDFS）
     */
    public boolean isOssProtocol() {
        return info.isOssProtocol();
    }

    /**
     * 关闭UFS文件系统
     */
    public void close() throws IOException {
        if (ufs != null) {
            ufs.close();
        }
    }

    // Getters
    public MountInfo getInfo() {
        return info;
    }

    public FileSystem getUfs() {
        return ufs;
    }

    @Override
    public String toString() {
        return "MountValue{" +
                "cvPath='" + info.getCvPath() + '\'' +
                ", ufsPath='" + info.getUfsPath() + '\'' +
                ", mountId='" + mountId + '\'' +
                '}';
    }
}
