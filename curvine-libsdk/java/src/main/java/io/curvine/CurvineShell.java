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

import io.curvine.bench.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;
import com.google.protobuf.util.JsonFormat;

// curvine command line tool, is a wrapper class for hdfs fs shell
public class CurvineShell {
    public static void main(String[] args) throws Exception {
        String cmd;
        if (args.length == 0) {
            cmd = "";
        } else {
            cmd = args[0];
        }

        String[] params = new String[] {};
        if (args.length > 1) {
            params = new String[args.length - 1];
            System.arraycopy(args, 1, params, 0, args.length - 1);
        }

        switch (cmd) {
            case "fs":
                runFsShell(params);
                break;

            case "mount":
                runMountCommand(params);
                break;

            case "umount":
                runUmountCommand(params);
                break;

            case "report":
                runReport(params);
                break;

            default:
                throw new RuntimeException("Unsupported operation type:" + args[0]);
        }
    }

    public static void runFsShell(String[] args) throws Exception {
        Configuration conf = Utils.getCurvineConf();
        conf.setQuietMode(false);
        
        // 统一使用OSSUnifiedFilesystem（仅支持oss-hdfs挂载点，其他挂载点回退到CurvineFileSystem逻辑）
        conf.set("fs.cv.impl", "io.curvine.OSSUnifiedFilesystem");
        System.setProperty("curvine.filesystem.type", "dfs");

        FsShell shell = new FsShell(conf);
        int res;
        try {
            res = ToolRunner.run(shell, args);
        } finally {
            shell.close();
        }
        System.exit(res);
    }

    public static void runMountCommand(String[] args) throws Exception {
        Configuration conf = Utils.getCurvineConf();
        conf.setQuietMode(false);
        
        // 统一使用OSSUnifiedFilesystem支持mount操作
        conf.set("fs.cv.impl", "io.curvine.OSSUnifiedFilesystem");
        System.setProperty("curvine.filesystem.type", "dfs");
        
        // 解析mount命令参数
        if (args.length < 2) {
            System.err.println("Usage: mount <ufs_path> <cv_path> [--oss-hdfs-enable] [--config key=value]...");
            System.exit(1);
            return;
        }
        
        String ufsPath = args[0];
        String cvPath = args[1];
        boolean ossHdfsEnabled = false;
        java.util.Map<String, String> configs = new java.util.HashMap<>();
        
        // 解析可选参数
        for (int i = 2; i < args.length; i++) {
            if ("--oss-hdfs-enable".equals(args[i])) {
                ossHdfsEnabled = true;
            } else if ("--config".equals(args[i]) && i + 1 < args.length) {
                // 解析 --config key=value
                String configPair = args[++i];
                String[] parts = configPair.split("=", 2);
                if (parts.length == 2) {
                    String key = parts[0].trim();
                    String value = parts[1].trim();
                    configs.put(key, value);
                    System.out.printf("解析配置: %s = %s%n", key, value);
                } else {
                    System.err.printf("警告: 无效的配置格式: %s，应为 key=value%n", configPair);
                }
            }
        }
        
        try (FileSystem fs = FileSystem.get(conf)) {
            if (fs instanceof OSSUnifiedFilesystem) {
                // 使用OSSUnifiedFilesystem的mount方法
                OSSUnifiedFilesystem ossFs = (OSSUnifiedFilesystem) fs;
                ossFs.mount(ufsPath, cvPath, ossHdfsEnabled, configs);
                System.out.printf("挂载成功: %s -> %s (%s协议)%n", 
                    cvPath, ufsPath, ossHdfsEnabled ? "oss-hdfs" : "标准");
                if (!configs.isEmpty()) {
                    System.out.println("应用的配置:");
                    configs.forEach((k, v) -> System.out.printf("  %s = %s%n", k, v));
                }
            } else {
                System.err.println("错误: 当前FileSystem不支持mount操作");
                System.exit(1);
            }
        }
        
        System.exit(0);
    }

    public static void runUmountCommand(String[] args) throws Exception {
        Configuration conf = Utils.getCurvineConf();
        conf.setQuietMode(false);
        
        // 统一使用OSSUnifiedFilesystem支持umount操作
        conf.set("fs.cv.impl", "io.curvine.OSSUnifiedFilesystem");
        System.setProperty("curvine.filesystem.type", "dfs");
        
        // 解析umount命令参数
        if (args.length < 1) {
            System.err.println("Usage: umount <cv_path>");
            System.exit(1);
            return;
        }
        
        String cvPath = args[0];
        
        try (FileSystem fs = FileSystem.get(conf)) {
            if (fs instanceof OSSUnifiedFilesystem) {
                // 使用OSSUnifiedFilesystem的umount方法
                OSSUnifiedFilesystem ossFs = (OSSUnifiedFilesystem) fs;
                ossFs.umount(cvPath);
                System.out.printf("卸载成功: %s%n", cvPath);
            } else {
                System.err.println("错误: 当前FileSystem不支持umount操作");
                System.exit(1);
            }
        }
        
        System.exit(0);
    }

    public static void runReport(String[] args) throws Exception {
        Configuration conf = Utils.getCurvineConf();
        conf.setQuietMode(false);

        try (FileSystem fs = FileSystem.get(conf)) {
            CurvineFsStat stat = ((CurvineFileSystem) fs).getFsStat();
            if (args.length == 0) {
                System.out.println(stat.simple(false));
            } else {
                String action = args[0].toLowerCase();
                switch (action) {
                    case "json":
                        System.out.println(JsonFormat.printer().print(stat.getInfo()));
                        break;
                    case "info":
                        System.out.println(stat.simple(true));
                        break;
                    case "capacity":
                        System.out.println(stat.capacity());
                        break;
                    case "used":
                        System.out.println(stat.used());
                        break;
                    case "available":
                        System.out.println(stat.available());
                        break;
                    default:
                        System.out.println(stat.simple(false));
                        break;
                }
            }
        }
        System.exit(0);
    }
}
