# Mount命令配置支持

## 功能概述

扩展了 `dfs mount` 命令，支持通过 `--config` 参数传递OSS相关配置，解决了需要预先配置OSS凭证的问题。

## 命令格式

```bash
dfs mount <ufs_path> <cv_path> [--oss-hdfs-enable] [--config key=value]...
```

### 参数说明

- `<ufs_path>`: UFS路径 (如 `oss://bucket/path`)
- `<cv_path>`: Curvine路径 (如 `/path`)  
- `--oss-hdfs-enable`: 启用OSS-HDFS协议支持
- `--config key=value`: 配置参数，可多次使用

## 支持的配置项

| 配置键 | 说明 | 示例值 |
|--------|------|--------|
| `oss.endpoint_url` | OSS Endpoint地址 | `https://oss-cn-hangzhou.aliyuncs.com` |
| `oss.credentials.access` | AccessKey ID | `LTAI4G8Z9QK2EXAMPLE` |
| `oss.credentials.secret` | AccessKey Secret | `k7f2vF9QK2ExampleSecretKey` |

## 使用示例

### 基本挂载
```bash
# 标准OSS挂载
dfs mount oss://mybucket/logs/ /logs/

# OSS-HDFS挂载
dfs mount oss://mybucket/logs/ /logs/ --oss-hdfs-enable
```

### 带配置的挂载
```bash
# 单个配置
dfs mount oss://mybucket/logs/ /logs/ \
    --oss-hdfs-enable \
    --config oss.endpoint_url=https://oss-cn-hangzhou.aliyuncs.com

# 多个配置
dfs mount oss://mybucket/logs/ /logs/ \
    --oss-hdfs-enable \
    --config oss.endpoint_url=https://oss-cn-hangzhou.aliyuncs.com \
    --config oss.credentials.access=LTAI4G8Z9QK2EXAMPLE \
    --config oss.credentials.secret=k7f2vF9QK2ExampleSecretKey
```

### 完整示例
```bash
# 生产环境挂载示例
dfs mount oss://production-logs/app-logs/ /production/logs/ \
    --oss-hdfs-enable \
    --config oss.endpoint_url=https://oss-cn-beijing.aliyuncs.com \
    --config oss.credentials.access=LTAI4GCH7M9KEXAMPLE \
    --config oss.credentials.secret=k7f2vF9QK2ExampleProductionKey
```

## 实现细节

### 代码修改

1. **CurvineShell.java**:
   - 扩展 `runMountCommand()` 参数解析
   - 支持 `--config key=value` 格式
   - 传递配置到 `OSSUnifiedFilesystem`

2. **OSSUnifiedFilesystem.java**:
   - 添加 `mount()` 重载方法
   - 接收并处理额外配置参数
   - 将配置传递到JNI层

### 配置流向

```
命令行 --config 参数
    ↓
CurvineShell 解析
    ↓
OSSUnifiedFilesystem.mount()
    ↓
JNI → Rust mount()
    ↓
Master存储挂载信息
```

## 错误处理

### 常见错误

1. **配置格式错误**:
   ```
   警告: 无效的配置格式: oss.endpoint_url，应为 key=value
   ```

2. **缺少必要参数**:
   ```
   Usage: mount <ufs_path> <cv_path> [--oss-hdfs-enable] [--config key=value]...
   ```

### 调试信息

挂载成功时会显示：
```
解析配置: oss.endpoint_url = https://oss-cn-hangzhou.aliyuncs.com
解析配置: oss.credentials.access = LTAI4G8Z9QK2EXAMPLE
挂载成功: /logs/ -> oss://mybucket/logs/ (oss-hdfs协议)
应用的配置:
  oss.endpoint_url = https://oss-cn-hangzhou.aliyuncs.com
  oss.credentials.access = LTAI4G8Z9QK2EXAMPLE
```

## 安全建议

1. **凭证保护**: 避免在命令历史中暴露AccessKey Secret
2. **环境隔离**: 生产和测试环境使用不同的凭证
3. **最小权限**: OSS凭证应只授予必要的权限

## 兼容性

- ✅ 向后兼容: 不带 `--config` 的命令照常工作
- ✅ 参数顺序: `--config` 和 `--oss-hdfs-enable` 可以任意顺序
- ✅ 多次配置: 支持多个 `--config` 参数
