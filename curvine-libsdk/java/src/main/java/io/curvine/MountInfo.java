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

import io.curvine.proto.MountInfoProto;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;

/**
 * Java端的MountInfo类，对应Rust中的MountInfo结构
 * 不直接使用Proto结构，而是作为内部数据结构
 */
public class MountInfo {
    private String cvPath;
    private String ufsPath;
    private int mountId;
    private Map<String, String> properties;
    private long ttlMs;
    private TtlAction ttlAction;
    private ConsistencyStrategy consistencyStrategy;
    private StorageType storageType;
    private long blockSize;
    private int replicas;
    private MountType mountType;

    /**
     * 从Protobuf对象创建MountInfo
     */
    public static MountInfo fromProto(MountInfoProto proto) {
        MountInfo info = new MountInfo();
        info.cvPath = proto.getCvPath();
        info.ufsPath = proto.getUfsPath();
        info.mountId = (int) proto.getMountId();
        info.properties = new HashMap<>(proto.getPropertiesMap());
        info.ttlMs = proto.getTtlMs();
        info.ttlAction = TtlAction.fromProto(proto.getTtlAction());
        info.consistencyStrategy = ConsistencyStrategy.fromProto(proto.getConsistencyStrategy());
        
        if (proto.hasStorageType()) {
            info.storageType = StorageType.fromProto(proto.getStorageType());
        }
        if (proto.hasBlockSize()) {
            info.blockSize = proto.getBlockSize();
        }
        if (proto.hasReplicas()) {
            info.replicas = proto.getReplicas();
        }
        info.mountType = MountType.fromProto(proto.getMountType());
        
        return info;
    }

    /**
     * 获取UFS路径，根据CV路径计算对应的UFS路径
     */
    public String getUfsPath(String cvPath) {
        String mountCvPath = this.cvPath;
        String mountUfsPath = this.ufsPath;
        
        // 确保mountCvPath以/结尾
        if (!mountCvPath.endsWith("/")) {
            mountCvPath += "/";
        }
        
        // 确保mountUfsPath以/结尾  
        if (!mountUfsPath.endsWith("/")) {
            mountUfsPath += "/";
        }
        
        // 计算相对路径
        String relativePath = "";
        if (cvPath.length() > mountCvPath.length()) {
            relativePath = cvPath.substring(mountCvPath.length());
        }
        
        return mountUfsPath + relativePath;
    }

    /**
     * 检查是否支持自动缓存
     */
    public boolean autoCache() {
        return properties.getOrDefault("curvine.auto.cache", "false").equals("true");
    }

    /**
     * 检查是否启用了OSS-HDFS协议
     */
    public boolean isOssHdfsEnabled() {
        return properties.getOrDefault("oss-hdfs-enable", "false").equals("true") ||
               properties.getOrDefault("fs.type", "oss").equals("oss-hdfs");
    }

    /**
     * 检查是否是OSS协议（包括OSS和OSS-HDFS）
     */
    public boolean isOssProtocol() {
        return ufsPath != null && ufsPath.startsWith("oss://");
    }

    // Getters and Setters
    public String getCvPath() {
        return cvPath;
    }

    public void setCvPath(String cvPath) {
        this.cvPath = cvPath;
    }

    public String getUfsPath() {
        return ufsPath;
    }

    public void setUfsPath(String ufsPath) {
        this.ufsPath = ufsPath;
    }

    public int getMountId() {
        return mountId;
    }

    public void setMountId(int mountId) {
        this.mountId = mountId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public long getTtlMs() {
        return ttlMs;
    }

    public void setTtlMs(long ttlMs) {
        this.ttlMs = ttlMs;
    }

    public TtlAction getTtlAction() {
        return ttlAction;
    }

    public void setTtlAction(TtlAction ttlAction) {
        this.ttlAction = ttlAction;
    }

    public ConsistencyStrategy getConsistencyStrategy() {
        return consistencyStrategy;
    }

    public void setConsistencyStrategy(ConsistencyStrategy consistencyStrategy) {
        this.consistencyStrategy = consistencyStrategy;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(StorageType storageType) {
        this.storageType = storageType;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public MountType getMountType() {
        return mountType;
    }

    public void setMountType(MountType mountType) {
        this.mountType = mountType;
    }

    @Override
    public String toString() {
        return "MountInfo{" +
                "cvPath='" + cvPath + '\'' +
                ", ufsPath='" + ufsPath + '\'' +
                ", mountId=" + mountId +
                ", properties=" + properties +
                ", ttlMs=" + ttlMs +
                ", ttlAction=" + ttlAction +
                ", consistencyStrategy=" + consistencyStrategy +
                ", storageType=" + storageType +
                ", blockSize=" + blockSize +
                ", replicas=" + replicas +
                ", mountType=" + mountType +
                '}';
    }
}
