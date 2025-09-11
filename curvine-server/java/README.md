# Curvine OSS Sync Worker

Java组件，负责将OSS/OSS-HDFS数据同步到Curvine文件系统。运行在独立JVM进程中，由Rust Worker管理生命周期。

## 架构

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Rust Worker   │    │   Java Process   │    │   External      │
│                 │    │                  │    │   Systems       │
│  LoadTaskRunner │───▶│ OssDataSyncRunner│───▶│                 │
│  JvmProcess     │    │  ├─OssDataReader │    │  ├─OSS/HDFS     │
│  Manager        │◀───│  └─CurvineWriter │────┼─▶└─Curvine      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

**工作流程**：
1. Rust Worker检测到OSS schema任务，启动Java进程
2. Java进程使用JindoData/Hadoop连接器读取OSS数据
3. 通过curvine-hadoop客户端写入Curvine集群
4. 定期向Rust进程报告进度和状态

## 配置

### Worker配置 (curvine-oss-worker.toml)

```toml
[worker]
# JVM配置
java_home = "/usr/lib/jvm/java-8-openjdk-amd64"
jvm_heap_size = "2g"
jvm_max_retries = 3
jvm_process_timeout_ms = 600000
```

### OSS挂载配置

OSS配置通过 `cv mount` 命令传递给Master，再由Master分配给Worker：

```bash
# 挂载OSS存储时配置连接参数
cv mount oss://my-bucket/data /curvine/oss-data \
  --oss.endpoint https://oss-cn-beijing.aliyuncs.com \
  --oss.accessKeyId your-access-key \
  --oss.accessKeySecret your-secret-key
```

### 运行时参数

Java进程从Worker接收任务配置：
- `--task-id`: 任务ID
- `--source-path`: OSS源路径 (来自mount配置)
- `--target-path`: Curvine目标路径
- `--oss-config`: OSS连接配置 (来自mount时的配置)
- `--curvine-config`: Curvine集群配置

### 单独启动JVM进程

用于测试和调试，可以直接启动Java同步进程：

```bash
java -Xmx2g -cp curvine-oss-sync-1.0.0.jar \
  io.curvine.worker.OssDataSyncRunner \
  --task-id "task-001" \
  --source-path "oss://my-bucket/data/" \
  --target-path "/curvine/data/" \
  --oss-config '{"fs.oss.endpoint":"https://oss-cn-beijing.aliyuncs.com","fs.oss.accessKeyId":"your-key","fs.oss.accessKeySecret":"your-secret"}' \
  --curvine-config '{"curvine.master.address":"localhost:8070","curvine.replicas":"3","curvine.block.size":"67108864"}'
```

**必需参数**：
- `--task-id`: 唯一任务标识符
- `--source-path`: OSS源路径，格式为 `oss://bucket/path/`
- `--target-path`: Curvine目标路径
- `--oss-config`: OSS配置，JSON格式字符串
- `--curvine-config`: Curvine配置，JSON格式字符串


## 构建

```bash
# 单独构建
cd curvine-server/java
mvn clean package -DskipTests

# 集成构建
cd curvine-root
make build --package server
```

构建产物：
- `target/curvine-oss-sync-1.0.0.jar` (可执行JAR，包含所有依赖)
- 自动复制到 `build/dist/lib/` 目录

## 依赖

- **Java 8+**：运行环境
- **curvine-hadoop**：Curvine文件系统客户端
- **hadoop-aliyun**：标准OSS连接器 (当JindoData不可用时)
- **JindoData** (可选)：阿里云官方OSS-HDFS连接器

组件会自动检测JindoData是否可用，不可用时降级使用标准Hadoop OSS连接器。