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

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件系统工厂类，根据命令类型创建相应的FileSystem实例
 * 支持cv和dfs命令的区分逻辑
 */
public class FilesystemFactory {
    public static final Logger LOGGER = LoggerFactory.getLogger(FilesystemFactory.class);

    /**
     * 根据命令类型创建FileSystem实例
     * @param isDfsCommand 是否是dfs命令
     * @return 对应的FileSystem实例
     */
    public static FileSystem createFileSystem(boolean isDfsCommand) {
        if (isDfsCommand) {
            LOGGER.info("创建OSSUnifiedFilesystem用于dfs命令 (支持oss-hdfs协议)");
            return new OSSUnifiedFilesystem(true);
        } else {
            LOGGER.info("创建CurvineFileSystem用于cv命令 (仅支持oss协议)");
            return new CurvineFileSystem();
        }
    }

    /**
     * 检测当前是否为dfs命令
     * 通过检查系统属性、环境变量或调用栈来判断
     */
    public static boolean isDfsCommand() {
        // 方法1：检查系统属性
        String commandType = System.getProperty("curvine.command.type");
        if ("dfs".equals(commandType)) {
            return true;
        } else if ("cv".equals(commandType)) {
            return false;
        }

        // 方法2：检查环境变量
        String envCommandType = System.getenv("CURVINE_COMMAND_TYPE");
        if ("dfs".equals(envCommandType)) {
            return true;
        } else if ("cv".equals(envCommandType)) {
            return false;
        }

        // 方法3：检查调用栈
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTrace) {
            String className = element.getClassName();
            String methodName = element.getMethodName();
            
            // 检查是否通过dfs脚本调用
            if (className.contains("dfs") || 
                methodName.contains("dfs") ||
                className.contains("DfsShell")) {
                return true;
            }
        }

        // 方法4：检查程序参数
        String[] args = System.getProperty("sun.java.command", "").split("\\s+");
        for (String arg : args) {
            if (arg.contains("dfs") || arg.contains("DfsShell")) {
                return true;
            }
        }

        // 默认情况下假设是cv命令
        LOGGER.debug("无法确定命令类型，默认使用cv命令模式");
        return false;
    }

    /**
     * 注意：OSS-HDFS的验证应该基于MountInfo的properties而非URL格式进行
     * 因为OSS和OSS-HDFS都使用相同的oss://格式
     * 实际验证请使用OSSUnifiedFilesystem中的validateOssHdfsAccess方法
     */
}
