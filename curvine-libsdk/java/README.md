# Curvine Java SDK

## 项目架构

### 核心组件
- **CurvineFileSystem**: Hadoop兼容的文件系统实现
- **OSSUnifiedFilesystem**: 支持OSS和OSS-HDFS的统一文件系统  
- **CurvineShell**: 命令行工具支持 (mount/umount/fs操作)
- **MountCache**: 挂载点缓存管理
- **JNI绑定**: Java与Rust核心的桥接

### 关键类
```
io.curvine/
├── CurvineFileSystem.java          # Hadoop文件系统接口实现
├── OSSUnifiedFilesystem.java       # 统一文件系统（支持oss-hdfs）
├── CurvineShell.java              # shell命令支持
├── MountCache.java                # 挂载点缓存
├── MountInfo.java                 # 挂载信息结构
├── MountValue.java                # 挂载值包装
└── CurvineNative.java             # JNI本地方法声明
```

### 测试用例
- **BenchTest**: 性能基准测试
- **CurvineFileSystemTest**: 文件系统功能测试
- **LibFsTest**: 底层文件系统测试  
- **ShellTest**: Shell命令测试

### 编译和运行
```bash
mvn clean compile                   # 编译
mvn test                           # 运行测试
```

### OSS-HDFS支持
- 通过 `--oss-hdfs-enable` 参数区分OSS和OSS-HDFS协议
- `bin/dfs` 支持OSS-HDFS，`bin/cv` 仅支持标准OSS
- 使用JindoOssFileSystem处理OSS-HDFS挂载点

### 注意事项
- 编译后的.class文件位于 `target/classes/` 目录
- JNI库文件位于 `native/` 目录
- protobuf生成的文件位于 `target/generated-sources/protobuf/java/`