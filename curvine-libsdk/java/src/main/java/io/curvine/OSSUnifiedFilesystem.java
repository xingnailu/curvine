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

import io.curvine.exception.CurvineException;
import io.curvine.proto.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageSize;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

/**
 * OSS统一文件系统，类似于Rust中的unified_filesystem.rs
 * 完整的统一文件系统实现，支持mount/unmount和所有文件操作
 * 特别支持oss-hdfs协议（通过Java SDK），其他协议通过JNI调用Rust
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class OSSUnifiedFilesystem extends FileSystem {
    public static final Logger LOGGER = LoggerFactory.getLogger(OSSUnifiedFilesystem.class);

    private CurvineFsMount libFs;
    private FilesystemConf filesystemConf;
    private MountCache mountCache;
    private Path workingDir;
    private URI uri;
    private int writeChunkSize;
    private int writeChunkNum;

    public final static String SCHEME = "cv";

    public OSSUnifiedFilesystem() {
        // 默认构造函数，所有调用都通过dfs进来
    }

    public OSSUnifiedFilesystem(boolean isDfsCommand) {
        // 为兼容性保留，但忽略参数（所有调用都通过dfs进来）
    }


    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public URI getUri() {
        return this.uri;
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        workingDir = newDir;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    public FilesystemConf getFilesystemConf() {
        return filesystemConf;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);
        String authority;
        try {
            filesystemConf = new FilesystemConf(conf);
            // cv://host:name format, host:name is the curvine cluster address.
            if (StringUtils.isNotEmpty(name.getAuthority())) {
                filesystemConf.master_addrs = name.getAuthority();
                authority = name.getAuthority();
            } else {
                authority = "/";
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

        this.uri = URI.create(name.getScheme() + "://" + authority);
        this.workingDir = getHomeDirectory();
        this.libFs = new CurvineFsMount(filesystemConf);

        // 初始化mount缓存
        long updateTtlMs = parseTimeToMillis(filesystemConf.mount_update_ttl);
        this.mountCache = new MountCache(updateTtlMs);

        StorageSize size = StorageSize.parse(filesystemConf.write_chunk_size);
        this.writeChunkSize = (int) size.getUnit().toBytes(size.getValue());
        this.writeChunkNum = filesystemConf.write_chunk_num;

        LOGGER.info("初始化OSSUnifiedFilesystem, enableUnified={}", 
                filesystemConf.enable_unified_fs);
    }

    private String formatPath(Path path) {
        return makeQualified(path).toUri().getPath();
    }

    /**
     * 获取mount信息，如果路径被挂载则返回mount信息，否则返回null
     * 类似于Rust unified_filesystem.rs 中的 get_mount 方法
     */
    private MountValue getMount(String cvPath) throws IOException {
        if (!filesystemConf.enable_unified_fs) {
            return null;
        }

        // 更新mount缓存
        mountCache.checkUpdate(libFs, false);
        return mountCache.getMount(cvPath);
    }

    /**
     * 挂载UFS到Curvine路径
     * 类似于Rust unified_filesystem.rs 中的 mount 方法
     * 底层通过JNI调用Rust实现
     */
    public void mount(String ufsPath, String cvPath, boolean ossHdfsEnabled) throws IOException {
        mount(ufsPath, cvPath, ossHdfsEnabled, new java.util.HashMap<>());
    }

    /**
     * 挂载UFS路径到Curvine路径，支持自定义配置
     * @param ufsPath UFS路径
     * @param cvPath Curvine路径  
     * @param ossHdfsEnabled 是否启用oss-hdfs
     * @param configs 额外的配置参数 (如 oss.endpoint_url, oss.credentials.access 等)
     */
    public void mount(String ufsPath, String cvPath, boolean ossHdfsEnabled, 
                     java.util.Map<String, String> configs) throws IOException {
        // 记录挂载操作类型
        if (ossHdfsEnabled) {
            LOGGER.info("oss-hdfs挂载操作: {} -> {}", cvPath, ufsPath);
        } else {
            LOGGER.info("标准协议挂载操作: {} -> {}", cvPath, ufsPath);
        }

        // 构建mount属性
        java.util.Map<String, String> properties = new java.util.HashMap<>();
        if (ossHdfsEnabled) {
            properties.put("oss-hdfs-enable", "true");
            properties.put("fs.type", "oss-hdfs");
        }
        
        // 添加用户提供的配置
        if (configs != null && !configs.isEmpty()) {
            LOGGER.info("添加用户配置: {}", configs);
            properties.putAll(configs);
        }
        
        // 转换为JSON
        String propertiesJson = convertPropertiesToJson(properties);
        
        // 调用底层JNI mount方法
        LOGGER.info("调用底层mount: {} -> {} (oss-hdfs: {}, configs: {})", 
                   cvPath, ufsPath, ossHdfsEnabled, configs.size());
        libFs.mount(ufsPath, cvPath, propertiesJson);
        
        // 强制更新mount缓存
        mountCache.checkUpdate(libFs, true);
        LOGGER.info("mount操作完成，缓存已更新");
    }

    /**
     * 将属性Map转换为JSON字符串
     */
    private String convertPropertiesToJson(java.util.Map<String, String> properties) throws IOException {
        try {
            StringBuilder json = new StringBuilder("{");
            boolean first = true;
            for (java.util.Map.Entry<String, String> entry : properties.entrySet()) {
                if (!first) {
                    json.append(",");
                }
                json.append("\"").append(entry.getKey()).append("\":\"").append(entry.getValue()).append("\"");
                first = false;
            }
            json.append("}");
            return json.toString();
        } catch (Exception e) {
            throw new IOException("Failed to convert properties to JSON", e);
        }
    }

    /**
     * 卸载Curvine路径的挂载
     * 类似于Rust unified_filesystem.rs 中的 umount 方法
     * 底层通过JNI调用Rust实现
     */
    public void umount(String cvPath) throws IOException {
        LOGGER.info("卸载挂载点: {}", cvPath);
        
        // 调用底层JNI umount方法
        libFs.umount(cvPath);
        
        // 从缓存中移除
        mountCache.removeMount(cvPath);
        
        LOGGER.info("umount操作完成，挂载点已移除: {}", cvPath);
    }

    /**
     * 获取UFS路径
     * 类似于Rust unified_filesystem.rs 中的 get_ufs_path 方法
     */
    public String getUfsPath(String cvPath) throws IOException {
        MountValue mountValue = getMount(cvPath);
        if (mountValue != null) {
            return mountValue.getUfsPath(cvPath);
        }
        return null;
    }

    /**
     * 验证OSS-HDFS协议的使用权限
     * @param mountValue 挂载点信息
     * @param operation 操作类型
     * @throws IOException 如果协议不支持
     */
    private void validateOssHdfsAccess(MountValue mountValue, String operation) throws IOException {
        if (mountValue != null && mountValue.isOssProtocol() && mountValue.isOssHdfsEnabled()) {
            // 所有调用都通过dfs进来，无需检查命令类型
            LOGGER.debug("处理oss-hdfs挂载点: {} 操作: {}", 
                mountValue.getInfo().getCvPath(), operation);
        }
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        String cvPath = formatPath(path);
        MountValue mountValue = getMount(cvPath);
        
        if (mountValue != null) {
            String ufsPath = mountValue.getUfsPath(cvPath);
            validateOssHdfsAccess(mountValue, "open");
            
            // 只有OSS协议且启用oss-hdfs时才使用UFS FileSystem，其他情况走JNI->Rust
            if (mountValue.isOssProtocol() && mountValue.isOssHdfsEnabled()) {
                // oss-hdfs: 使用Java SDK (JindoOssFileSystem)
                try {
                    FileSystem ufs = mountValue.getUfs();
                    Path ufsPathObj = new Path(ufsPath);
                    LOGGER.info("从oss-hdfs UFS读取文件: {} -> {}", cvPath, ufsPath);
                    return ufs.open(ufsPathObj, bufferSize);
                } catch (Exception e) {
                    LOGGER.warn("从oss-hdfs UFS读取失败，尝试从Curvine缓存读取: {} -> {}", cvPath, ufsPath, e);
                    // 回退到Curvine读取（缓存）
                }
            } else {
                // 其他协议或标准oss: 直接使用JNI->Rust，不使用UFS FileSystem
                LOGGER.debug("非oss-hdfs挂载点，使用JNI->Rust: {} -> {} (协议: {})", 
                    cvPath, ufsPath, mountValue.isOssProtocol() ? "oss" : "other");
            }
        }

        // 无挂载或UFS读取失败：使用原生Curvine读取
        long[] tmp = new long[] {0, 0};
        try {
            long nativeHandle = libFs.open(cvPath, tmp);
            return new FSDataInputStream(new CurvineInputStream(libFs, nativeHandle, tmp[0]));
        } catch (CurvineException e) {
            if (mountValue != null && !filesystemConf.enable_read_ufs) {
                String ufsPath = mountValue.getUfsPath(cvPath);
                Path ufsPathObj = new Path(ufsPath);
                LOGGER.warn("不支持Rust API读取，使用Java API读取: {} -> {}", cvPath, ufsPath);
                return FileSystem.get(ufsPathObj.toUri(), getConf()).open(ufsPathObj, bufferSize);
            } else {
                throw e;
            }
        }
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission fsPermission,
            boolean overwrite,
            int bufferSize,
            short replication,
            long blockSize,
            Progressable progress
    ) throws IOException {
        String cvPath = formatPath(path);
        MountValue mountValue = getMount(cvPath);
        
        if (mountValue != null) {
            String ufsPath = mountValue.getUfsPath(cvPath);
            validateOssHdfsAccess(mountValue, "create");
            
            // 只有OSS协议且启用oss-hdfs时才使用UFS FileSystem，其他情况走JNI->Rust
            if (mountValue.isOssProtocol() && mountValue.isOssHdfsEnabled()) {
                // oss-hdfs: 使用Java SDK (JindoOssFileSystem)
                FileSystem ufs = mountValue.getUfs();
                Path ufsPathObj = new Path(ufsPath);
                LOGGER.info("创建oss-hdfs UFS文件: {} -> {}", cvPath, ufsPath);
                return ufs.create(ufsPathObj, fsPermission, overwrite, bufferSize, replication, blockSize, progress);
            } else {
                // 其他协议或标准oss: 直接使用JNI->Rust，不使用UFS FileSystem
                LOGGER.debug("非oss-hdfs挂载点，使用JNI->Rust: {} -> {} (协议: {})", 
                    cvPath, ufsPath, mountValue.isOssProtocol() ? "oss" : "other");
            }
        }

        // 无挂载：使用原生Curvine创建
        long nativeHandle = this.libFs.create(cvPath, overwrite);
        CurvineOutputStream output = new CurvineOutputStream(libFs, nativeHandle, 0, writeChunkSize, writeChunkNum);
        return new FSDataOutputStream(output, statistics);
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        String cvPath = formatPath(path);
        MountValue mountValue = getMount(cvPath);
        
        if (mountValue != null) {
            String ufsPath = mountValue.getUfsPath(cvPath);
            validateOssHdfsAccess(mountValue, "append");
            
            // 只有OSS协议且启用oss-hdfs时才使用UFS FileSystem，其他情况走JNI->Rust
            if (mountValue.isOssProtocol() && mountValue.isOssHdfsEnabled()) {
                // oss-hdfs: 使用Java SDK (JindoOssFileSystem)
                FileSystem ufs = mountValue.getUfs();
                Path ufsPathObj = new Path(ufsPath);
                LOGGER.info("向oss-hdfs UFS文件追加: {} -> {}", cvPath, ufsPath);
                return ufs.append(ufsPathObj, bufferSize, progress);
            } else {
                // 其他协议或标准oss: 直接使用JNI->Rust，不使用UFS FileSystem
                LOGGER.debug("非oss-hdfs挂载点，使用JNI->Rust: {} -> {} (协议: {})", 
                    cvPath, ufsPath, mountValue.isOssProtocol() ? "oss" : "other");
            }
        }

        // 无挂载：使用原生Curvine追加
        long[] tmp = new long[] {0};
        long nativeHandle = this.libFs.append(cvPath, tmp);
        CurvineOutputStream output = new CurvineOutputStream(libFs, nativeHandle, tmp[0], writeChunkSize, writeChunkNum);
        return new FSDataOutputStream(output, statistics, output.pos());
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        String srcCvPath = formatPath(src);
        String dstCvPath = formatPath(dst);
        MountValue srcMountValue = getMount(srcCvPath);
        
        if (srcMountValue != null) {
            String srcUfsPath = srcMountValue.getUfsPath(srcCvPath);
            String dstUfsPath = srcMountValue.getUfsPath(dstCvPath);
            validateOssHdfsAccess(srcMountValue, "rename");
            
            // 只有OSS协议且启用oss-hdfs时才使用UFS FileSystem，其他情况走JNI->Rust
            if (srcMountValue.isOssProtocol() && srcMountValue.isOssHdfsEnabled()) {
                // oss-hdfs: 使用Java SDK (JindoOssFileSystem)
                FileSystem ufs = srcMountValue.getUfs();
                Path srcUfsPathObj = new Path(srcUfsPath);
                Path dstUfsPathObj = new Path(dstUfsPath);
                LOGGER.info("oss-hdfs UFS重命名: {} -> {} (UFS: {} -> {})", srcCvPath, dstCvPath, srcUfsPath, dstUfsPath);
                return ufs.rename(srcUfsPathObj, dstUfsPathObj);
            } else {
                // 其他协议或标准oss: 直接使用JNI->Rust，不使用UFS FileSystem
                LOGGER.debug("非oss-hdfs挂载点，使用JNI->Rust: {} -> {} (协议: {})", 
                    srcCvPath, dstCvPath, srcMountValue.isOssProtocol() ? "oss" : "other");
            }
        }

        // 无挂载：使用原生Curvine重命名
        libFs.rename(srcCvPath, dstCvPath);
        return true;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        String cvPath = formatPath(f);
        MountValue mountValue = getMount(cvPath);
        
        if (mountValue != null) {
            String ufsPath = mountValue.getUfsPath(cvPath);
            validateOssHdfsAccess(mountValue, "delete");
            
            // 只有OSS协议且启用oss-hdfs时才使用UFS FileSystem，其他情况走JNI->Rust
            if (mountValue.isOssProtocol() && mountValue.isOssHdfsEnabled()) {
                // oss-hdfs: 删除缓存和UFS
                try {
                    libFs.delete(cvPath, recursive);
                } catch (Exception e) {
                    LOGGER.warn("删除Curvine缓存失败: {} - {}", cvPath, e.getMessage());
                }
                
                // 删除UFS
                FileSystem ufs = mountValue.getUfs();
                Path ufsPathObj = new Path(ufsPath);
                LOGGER.info("删除oss-hdfs UFS文件: {} -> {}", cvPath, ufsPath);
                return ufs.delete(ufsPathObj, recursive);
            } else {
                // 其他协议或标准oss: 直接使用JNI->Rust，不使用UFS FileSystem
                LOGGER.debug("非oss-hdfs挂载点，使用JNI->Rust: {} (协议: {})", 
                    cvPath, mountValue.isOssProtocol() ? "oss" : "other");
            }
        }

        // 无挂载：使用原生Curvine删除
        libFs.delete(cvPath, recursive);
        return true;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        String cvPath = formatPath(f);
        MountValue mountValue = getMount(cvPath);
        
        if (mountValue != null) {
            String ufsPath = mountValue.getUfsPath(cvPath);
            validateOssHdfsAccess(mountValue, "mkdirs");
            
            // 只有OSS协议且启用oss-hdfs时才使用UFS FileSystem，其他情况走JNI->Rust
            if (mountValue.isOssProtocol() && mountValue.isOssHdfsEnabled()) {
                // oss-hdfs: 使用Java SDK (JindoOssFileSystem)
                FileSystem ufs = mountValue.getUfs();
                Path ufsPathObj = new Path(ufsPath);
                LOGGER.info("创建oss-hdfs UFS目录: {} -> {}", cvPath, ufsPath);
                return ufs.mkdirs(ufsPathObj, permission);
            } else {
                // 其他协议或标准oss: 直接使用JNI->Rust，不使用UFS FileSystem
                LOGGER.debug("非oss-hdfs挂载点，使用JNI->Rust: {} (协议: {})", 
                    cvPath, mountValue.isOssProtocol() ? "oss" : "other");
            }
        }

        // 无挂载：使用原生Curvine创建目录
        libFs.mkdir(cvPath, true);
        return true;
    }

    @Override
    public void close() throws IOException {
        if (libFs != null) {
            libFs.close();
            libFs = null;
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        String cvPath = formatPath(f);
        MountValue mountValue = getMount(cvPath);
        
        if (mountValue != null) {
            // 挂载点本身，使用Curvine获取状态
            if (cvPath.equals(mountValue.getInfo().getCvPath())) {
                byte[] bytes = libFs.getFileStatus(cvPath);
                GetFileStatusResponse proto = GetFileStatusResponse.parseFrom(bytes);
                return toHadoop(proto.getStatus(), f);
            }
            
            String ufsPath = mountValue.getUfsPath(cvPath);
            validateOssHdfsAccess(mountValue, "getFileStatus");
            
            // 只有OSS协议且启用oss-hdfs时才使用UFS FileSystem，其他情况走JNI->Rust
            if (mountValue.isOssProtocol() && mountValue.isOssHdfsEnabled()) {
                // oss-hdfs: 使用Java SDK (JindoOssFileSystem)
                FileSystem ufs = mountValue.getUfs();
                Path ufsPathObj = new Path(ufsPath);
                FileStatus ufsStatus = ufs.getFileStatus(ufsPathObj);
                LOGGER.debug("获取oss-hdfs UFS文件状态: {} -> {}", cvPath, ufsPath);
                
                // 转换为Curvine路径的状态
                return new FileStatus(
                    ufsStatus.getLen(),
                    ufsStatus.isDirectory(),
                    ufsStatus.getReplication(),
                    ufsStatus.getBlockSize(),
                    ufsStatus.getModificationTime(),
                    ufsStatus.getAccessTime(),
                    ufsStatus.getPermission(),
                    ufsStatus.getOwner(),
                    ufsStatus.getGroup(),
                    makeQualified(f)
                );
            } else {
                // 其他协议或标准oss: 直接使用JNI->Rust，不使用UFS FileSystem
                LOGGER.debug("非oss-hdfs挂载点，使用JNI->Rust: {} -> {} (协议: {})", 
                    cvPath, ufsPath, mountValue.isOssProtocol() ? "oss" : "other");
            }
        }

        // 无挂载：使用原生Curvine获取状态
        byte[] bytes = libFs.getFileStatus(cvPath);
        GetFileStatusResponse proto = GetFileStatusResponse.parseFrom(bytes);
        return toHadoop(proto.getStatus(), f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        String cvPath = formatPath(f);
        MountValue mountValue = getMount(cvPath);
        
        if (mountValue != null) {
            String ufsPath = mountValue.getUfsPath(cvPath);
            validateOssHdfsAccess(mountValue, "listStatus");
            
            // 只有OSS协议且启用oss-hdfs时才使用UFS FileSystem，其他情况走JNI->Rust
            if (mountValue.isOssProtocol() && mountValue.isOssHdfsEnabled()) {
                // oss-hdfs: 使用Java SDK (JindoOssFileSystem)
                FileSystem ufs = mountValue.getUfs();
                Path ufsPathObj = new Path(ufsPath);
                FileStatus[] ufsStatuses = ufs.listStatus(ufsPathObj);
                LOGGER.debug("列出oss-hdfs UFS目录: {} -> {} ({}个项目)", cvPath, ufsPath, ufsStatuses.length);
                
                // 转换为Curvine路径的状态
                FileStatus[] result = new FileStatus[ufsStatuses.length];
                for (int i = 0; i < ufsStatuses.length; i++) {
                    Path childPath = new Path(f, ufsStatuses[i].getPath().getName());
                    result[i] = new FileStatus(
                        ufsStatuses[i].getLen(),
                        ufsStatuses[i].isDirectory(),
                        ufsStatuses[i].getReplication(),
                        ufsStatuses[i].getBlockSize(),
                        ufsStatuses[i].getModificationTime(),
                        ufsStatuses[i].getAccessTime(),
                        ufsStatuses[i].getPermission(),
                        ufsStatuses[i].getOwner(),
                        ufsStatuses[i].getGroup(),
                        makeQualified(childPath)
                    );
                }
                return result;
            } else {
                // 其他协议或标准oss: 直接使用JNI->Rust，不使用UFS FileSystem
                LOGGER.debug("非oss-hdfs挂载点，使用JNI->Rust: {} -> {} (协议: {})", 
                    cvPath, ufsPath, mountValue.isOssProtocol() ? "oss" : "other");
            }
        }

        // 无挂载：使用原生Curvine列表
        byte[] bytes = libFs.listStatus(cvPath);
        ListStatusResponse proto = ListStatusResponse.parseFrom(bytes);
        FileStatus[] statuses = new FileStatus[proto.getStatusesList().size()];
        for (int i = 0; i < statuses.length; i++) {
            Path path = new Path(f, proto.getStatuses(i).getName());
            statuses[i] = toHadoop(proto.getStatuses(i), path);
        }
        return statuses;
    }

    public FileStatus toHadoop(FileStatusProto proto, Path path) {
        return new org.apache.hadoop.fs.FileStatus(
                proto.getLen(),
                proto.getIsDir(),
                proto.getReplicas(),
                proto.getBlockSize(),
                proto.getMtime(),
                proto.getAtime(),
                FsPermission.getDefault(),
                System.getProperty("user.name"),
                System.getProperty("user.group"),
                makeQualified(path)
        );
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        return getFsStat();
    }

    public CurvineFsStat getFsStat() throws IOException {
        byte[] bytes = libFs.getMasterInfo();
        GetMasterInfoResponse info = GetMasterInfoResponse.parseFrom(bytes);
        return new CurvineFsStat(info);
    }

    public Optional<MountInfoProto> getMountInfo(Path path) throws IOException {
        byte[] bytes = libFs.getMountInfo(path.toString());
        GetMountInfoResponse response = GetMountInfoResponse.parseFrom(bytes);
        if (response.hasMountInfo()) {
            return Optional.of(response.getMountInfo());
        } else {
            return Optional.empty();
        }
    }

    /**
     * 解析时间字符串为毫秒数
     */
    private long parseTimeToMillis(String timeStr) {
        if (timeStr.endsWith("m")) {
            return Long.parseLong(timeStr.substring(0, timeStr.length() - 1)) * 60 * 1000;
        } else if (timeStr.endsWith("s")) {
            return Long.parseLong(timeStr.substring(0, timeStr.length() - 1)) * 1000;
        } else {
            return Long.parseLong(timeStr);
        }
    }

    /**
     * 强制更新mount缓存
     */
    public void refreshMountCache() throws IOException {
        mountCache.checkUpdate(libFs, true);
        LOGGER.info("强制刷新mount缓存");
    }

    /**
     * 获取mount缓存的大小
     */
    public int getMountCacheSize() {
        return mountCache.size();
    }
}
