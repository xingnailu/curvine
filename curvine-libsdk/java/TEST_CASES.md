# 测试用例说明

## 单元测试

### BenchTest.java
- **功能**: 性能基准测试
- **测试点**: 
  - 文件读写性能
  - 并发访问性能
  - 大文件处理能力

### CurvineFileSystemTest.java
- **功能**: 文件系统基础功能测试
- **测试点**:
  - 文件创建、读取、写入、删除
  - 目录操作 (mkdir, list, rmdir)
  - 文件重命名和移动
  - 权限和属性设置

### LibFsTest.java
- **功能**: 底层文件系统JNI接口测试
- **测试点**:
  - JNI方法调用正确性
  - 内存管理和资源清理
  - 异常处理机制

### ShellTest.java  
- **功能**: Shell命令行工具测试
- **测试点**:
  - mount/umount命令
  - fs操作命令 (ls, mkdir, rm)
  - 参数解析和验证
  - OSS-HDFS特性支持

## 运行方式
```bash
# 运行所有测试
mvn test

# 运行单个测试类
mvn test -Dtest=CurvineFileSystemTest

# 运行特定测试方法
mvn test -Dtest=CurvineFileSystemTest#testCreateFile
```

## 测试配置
- 测试配置文件位于 `src/test/resources/`
- JNI库路径需要正确配置
- 确保Curvine服务已启动或使用Mock模式
