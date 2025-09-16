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

import io.curvine.proto.GetMountTableResponse;
import io.curvine.proto.MountInfoProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Java端的MountCache类，用于缓存mount信息，定期与Master同步数据
 * 类似于Rust中的MountCache实现
 * 使用内部结构而非直接使用Proto结构
 */
public class MountCache {
    public static final Logger LOGGER = LoggerFactory.getLogger(MountCache.class);
    
    private final long updateTtlMs;
    private final AtomicLong lastUpdate = new AtomicLong(0);
    private final ConcurrentHashMap<String, MountValue> mounts = new ConcurrentHashMap<>();
    
    public MountCache(long updateTtlMs) {
        this.updateTtlMs = updateTtlMs;
    }
    
    /**
     * 检查是否需要更新mount缓存
     */
    private boolean needUpdate() {
        long now = System.currentTimeMillis();
        return (now - lastUpdate.get()) > updateTtlMs;
    }
    
    /**
     * 更新mount缓存，从Master获取最新的mount table
     */
    public void checkUpdate(CurvineFsMount fsMount, boolean force) throws IOException {
        if (needUpdate() || force) {
            LOGGER.debug("开始更新mount缓存, force={}", force);
            
            try {
                // 通过native调用获取mount table
                byte[] bytes = fsMount.getMountTable();
                GetMountTableResponse response = GetMountTableResponse.parseFrom(bytes);
                
                // 关闭旧的UFS连接
                closeAllMounts();
                
                // 清空旧的缓存
                mounts.clear();
                
                // 重新构建缓存
                for (MountInfoProto mountInfoProto : response.getMountTableList()) {
                    try {
                        MountInfo mountInfo = MountInfo.fromProto(mountInfoProto);
                        MountValue mountValue = new MountValue(mountInfo);
                        mounts.put(mountInfo.getCvPath(), mountValue);
                    } catch (Exception e) {
                        LOGGER.warn("Failed to create MountValue for {}: {}", 
                            mountInfoProto.getCvPath(), e.getMessage());
                    }
                }
                
                lastUpdate.set(System.currentTimeMillis());
                LOGGER.debug("成功更新mount缓存，总计{}个挂载点", mounts.size());
                
            } catch (Exception e) {
                LOGGER.error("Failed to update mount cache", e);
                throw new IOException("Failed to update mount cache", e);
            }
        }
    }
    
    /**
     * 根据CV路径查找对应的mount信息
     * 实现最长匹配策略，类似Rust中的实现
     */
    public MountValue getMount(String cvPath) {
        MountValue bestMatch = null;
        String longestPrefix = "";
        
        for (MountValue mountValue : mounts.values()) {
            String mountPath = mountValue.getInfo().getCvPath();
            if (cvPath.startsWith(mountPath) && mountPath.length() > longestPrefix.length()) {
                longestPrefix = mountPath;
                bestMatch = mountValue;
            }
        }
        
        return bestMatch;
    }
    
    /**
     * 根据CV路径获取所有可能的挂载点路径（类似Rust中的get_possible_mounts）
     */
    public List<String> getPossibleMounts(String cvPath) {
        List<String> possibleMounts = new ArrayList<>();
        String[] parts = cvPath.split("/");
        StringBuilder currentPath = new StringBuilder();
        
        for (String part : parts) {
            if (!part.isEmpty()) {
                currentPath.append("/").append(part);
                possibleMounts.add(currentPath.toString());
            }
        }
        
        // 添加根路径
        if (!possibleMounts.contains("/")) {
            possibleMounts.add("/");
        }
        
        return possibleMounts;
    }
    
    /**
     * 获取UFS路径，根据CV路径计算对应的UFS路径
     */
    public String getUfsPath(String cvPath, MountValue mountValue) {
        return mountValue.getUfsPath(cvPath);
    }
    
    /**
     * 检查挂载点是否启用了OSS-HDFS协议
     * @param mountValue 挂载点信息
     * @return 是否启用OSS-HDFS
     */
    public boolean isOssHdfsEnabled(MountValue mountValue) {
        return mountValue != null && mountValue.isOssHdfsEnabled();
    }
    
    /**
     * 检查挂载点是否是OSS协议（包括OSS和OSS-HDFS）
     * @param mountValue 挂载点信息  
     * @return 是否是OSS协议
     */
    public boolean isOssProtocol(MountValue mountValue) {
        return mountValue != null && mountValue.isOssProtocol();
    }
    
    /**
     * 检查路径是否是OSS协议（仅用于URL检查）
     * @param ufsPath UFS路径
     * @return 是否以oss://开头
     */
    public boolean isOssPath(String ufsPath) {
        return ufsPath != null && ufsPath.startsWith("oss://");
    }
    
    /**
     * 移除指定路径的mount缓存
     */
    public void remove(String cvPath) {
        MountValue mountValue = mounts.remove(cvPath);
        if (mountValue != null) {
            try {
                mountValue.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close MountValue for {}: {}", cvPath, e.getMessage());
            }
            LOGGER.debug("移除mount缓存: {}", cvPath);
        }
    }
    
    /**
     * 获取缓存中的mount点数量
     */
    public int size() {
        return mounts.size();
    }
    
    /**
     * 关闭所有挂载点的UFS连接
     */
    private void closeAllMounts() {
        for (MountValue mountValue : mounts.values()) {
            try {
                mountValue.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close MountValue for {}: {}", 
                    mountValue.getInfo().getCvPath(), e.getMessage());
            }
        }
    }
    
    /**
     * 移除指定路径的挂载点
     * 类似于Rust MountCache的remove方法
     */
    public void removeMount(String cvPath) {
        MountValue removed = mounts.remove(cvPath);
        if (removed != null) {
            try {
                removed.close();
            } catch (Exception e) {
                LOGGER.warn("关闭挂载点FileSystem失败: {} - {}", cvPath, e.getMessage());
            }
            LOGGER.info("移除挂载点: {}", cvPath);
        } else {
            LOGGER.debug("挂载点不存在，无需移除: {}", cvPath);
        }
    }
    
    /**
     * 清理资源
     */
    public void cleanup() {
        closeAllMounts();
        mounts.clear();
        LOGGER.info("MountCache已清理");
    }
}
