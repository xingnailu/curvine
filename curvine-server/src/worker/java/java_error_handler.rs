// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Java异常类型
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JavaErrorType {
    /// JNI相关异常
    JniError,
    /// LibJVM库异常  
    LibJvmError,
    /// OutOfMemoryError
    OutOfMemory,
    /// ClassNotFoundException
    ClassNotFound,
    /// NoSuchMethodError
    NoSuchMethod,
    /// UnsatisfiedLinkError
    UnsatisfiedLink,
    /// IOException
    IoError,
    /// 网络连接异常
    NetworkError,
    /// OSS/HDFS连接异常
    StorageConnectionError,
    /// 认证异常
    AuthenticationError,
    /// 配置错误
    ConfigurationError,
    /// 进程退出
    ProcessExit,
    /// 进程错误
    ProcessError,
    /// 未知异常
    Unknown,
}

impl JavaErrorType {
    /// 判断是否为可重试的异常
    pub fn is_retryable(&self) -> bool {
        match self {
            JavaErrorType::NetworkError |
            JavaErrorType::StorageConnectionError |
            JavaErrorType::IoError |
            JavaErrorType::ProcessError => true,
            
            JavaErrorType::OutOfMemory |
            JavaErrorType::ClassNotFound |
            JavaErrorType::NoSuchMethod |
            JavaErrorType::UnsatisfiedLink |
            JavaErrorType::JniError |
            JavaErrorType::LibJvmError |
            JavaErrorType::AuthenticationError |
            JavaErrorType::ConfigurationError => false,
            
            JavaErrorType::ProcessExit |
            JavaErrorType::Unknown => false,
        }
    }

    /// 获取重试延迟时间（秒）
    pub fn retry_delay_seconds(&self) -> u64 {
        match self {
            JavaErrorType::NetworkError => 30,
            JavaErrorType::StorageConnectionError => 60,
            JavaErrorType::IoError => 10,
            JavaErrorType::ProcessError => 5,
            _ => 0,
        }
    }

    /// 获取最大重试次数
    pub fn max_retries(&self) -> u32 {
        match self {
            JavaErrorType::NetworkError => 5,
            JavaErrorType::StorageConnectionError => 3,
            JavaErrorType::IoError => 3,
            JavaErrorType::ProcessError => 2,
            _ => 0,
        }
    }
}

/// Java异常信息
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JavaError {
    pub error_type: JavaErrorType,
    pub message: String,
    pub stacktrace: Option<String>,
    pub timestamp: i64,
}

impl JavaError {
    pub fn new(error_type: JavaErrorType, message: String) -> Self {
        Self {
            error_type,
            message,
            stacktrace: None,
            timestamp: orpc::common::LocalTime::mills() as i64,
        }
    }

    pub fn with_stacktrace(mut self, stacktrace: String) -> Self {
        self.stacktrace = Some(stacktrace);
        self
    }

    /// 判断是否可以重试
    pub fn is_retryable(&self) -> bool {
        self.error_type.is_retryable()
    }

    /// 获取重试延迟时间
    pub fn retry_delay_seconds(&self) -> u64 {
        self.error_type.retry_delay_seconds()
    }

    /// 获取最大重试次数
    pub fn max_retries(&self) -> u32 {
        self.error_type.max_retries()
    }
}

/// Java异常处理器
#[derive(Debug, Clone)]
pub struct JavaErrorHandler {
    error_patterns: HashMap<JavaErrorType, Vec<Regex>>,
}

impl JavaErrorHandler {
    pub fn new() -> Self {
        let mut handler = Self {
            error_patterns: HashMap::new(),
        };
        handler.init_error_patterns();
        handler
    }

    /// 初始化异常模式匹配规则
    fn init_error_patterns(&mut self) {
        // JNI异常
        self.add_patterns(JavaErrorType::JniError, vec![
            r"JNI DETECTED ERROR",
            r"JNI WARNING",
            r"JNI ABORT",
            r"JNI_.*Error",
            r"java\.lang\.UnsupportedOperationException.*JNI",
        ]);

        // LibJVM异常
        self.add_patterns(JavaErrorType::LibJvmError, vec![
            r"LIBJVM: Fatal Error",
            r"libjvm\.so.*error",
            r"JVM_.*failed",
            r"SIGSEGV.*libjvm",
            r"Fatal Error.*JVM",
        ]);

        // OutOfMemoryError
        self.add_patterns(JavaErrorType::OutOfMemory, vec![
            r"java\.lang\.OutOfMemoryError",
            r"Out of memory",
            r"Cannot allocate memory",
            r"Java heap space",
            r"Metaspace",
            r"Direct buffer memory",
        ]);

        // ClassNotFoundException
        self.add_patterns(JavaErrorType::ClassNotFound, vec![
            r"java\.lang\.ClassNotFoundException",
            r"Could not find or load main class",
            r"ClassDefNotFoundError",
        ]);

        // NoSuchMethodError
        self.add_patterns(JavaErrorType::NoSuchMethod, vec![
            r"java\.lang\.NoSuchMethodError",
            r"java\.lang\.NoSuchFieldError",
            r"Method.*not found",
        ]);

        // UnsatisfiedLinkError
        self.add_patterns(JavaErrorType::UnsatisfiedLink, vec![
            r"java\.lang\.UnsatisfiedLinkError",
            r"Native library.*failed to load",
            r"Cannot load native library",
        ]);

        // IOException
        self.add_patterns(JavaErrorType::IoError, vec![
            r"java\.io\.IOException",
            r"java\.io\.FileNotFoundException",
            r"java\.nio\.file\.NoSuchFileException",
            r"Connection reset",
            r"Broken pipe",
        ]);

        // 网络异常
        self.add_patterns(JavaErrorType::NetworkError, vec![
            r"java\.net\.ConnectException",
            r"java\.net\.SocketTimeoutException",
            r"java\.net\.UnknownHostException",
            r"Connection timed out",
            r"Network is unreachable",
            r"Connection refused",
        ]);

        // OSS/HDFS连接异常
        self.add_patterns(JavaErrorType::StorageConnectionError, vec![
            r"com\.aliyun\.oss\.OSSException",
            r"com\.aliyun\.jindodata\..*Exception",
            r"org\.apache\.hadoop\..*Exception",
            r"Failed to connect to OSS",
            r"HDFS.*connection.*failed",
            r"Authentication failed.*OSS",
            r"Access denied.*OSS",
        ]);

        // 认证异常
        self.add_patterns(JavaErrorType::AuthenticationError, vec![
            r"Authentication failed",
            r"Access denied",
            r"Invalid credentials",
            r"Forbidden",
            r"Unauthorized",
            r"SignatureDoesNotMatch",
            r"InvalidAccessKeyId",
        ]);

        // 配置异常
        self.add_patterns(JavaErrorType::ConfigurationError, vec![
            r"Configuration error",
            r"Invalid configuration",
            r"Missing required property",
            r"IllegalArgumentException.*config",
        ]);
    }

    /// 添加异常模式
    fn add_patterns(&mut self, error_type: JavaErrorType, patterns: Vec<&str>) {
        let regex_patterns: Vec<Regex> = patterns
            .into_iter()
            .filter_map(|pattern| {
                Regex::new(pattern).map_err(|e| {
                    eprintln!("Failed to compile regex pattern '{}': {}", pattern, e);
                    e
                }).ok()
            })
            .collect();

        self.error_patterns.insert(error_type, regex_patterns);
    }

    /// 解析错误日志，返回匹配的Java异常
    pub fn parse_error(&self, log_line: &str) -> Option<JavaError> {
        for (error_type, patterns) in &self.error_patterns {
            for pattern in patterns {
                if pattern.is_match(log_line) {
                    return Some(JavaError::new(
                        error_type.clone(),
                        log_line.to_string(),
                    ));
                }
            }
        }

        // 如果没有匹配到特定模式，但包含Exception或Error关键字
        if log_line.contains("Exception") || log_line.contains("Error") {
            return Some(JavaError::new(
                JavaErrorType::Unknown,
                log_line.to_string(),
            ));
        }

        None
    }

    /// 解析多行异常堆栈
    pub fn parse_stacktrace(&self, lines: &[String]) -> Option<JavaError> {
        if lines.is_empty() {
            return None;
        }

        let first_line = &lines[0];
        if let Some(mut error) = self.parse_error(first_line) {
            let stacktrace = lines.join("\n");
            error = error.with_stacktrace(stacktrace);
            return Some(error);
        }

        None
    }

    /// 判断是否应该重启进程
    pub fn should_restart_process(&self, error: &JavaError, current_retries: u32) -> bool {
        if !error.is_retryable() {
            return false;
        }

        current_retries < error.max_retries()
    }

    /// 获取重试策略建议
    pub fn get_retry_strategy(&self, error: &JavaError) -> RetryStrategy {
        RetryStrategy {
            should_retry: error.is_retryable(),
            delay_seconds: error.retry_delay_seconds(),
            max_retries: error.max_retries(),
            exponential_backoff: matches!(
                error.error_type,
                JavaErrorType::NetworkError | JavaErrorType::StorageConnectionError
            ),
        }
    }
}

/// 重试策略
#[derive(Debug, Clone)]
pub struct RetryStrategy {
    pub should_retry: bool,
    pub delay_seconds: u64,
    pub max_retries: u32,
    pub exponential_backoff: bool,
}

impl RetryStrategy {
    /// 计算下次重试延迟时间
    pub fn calculate_delay(&self, retry_count: u32) -> u64 {
        if !self.exponential_backoff {
            return self.delay_seconds;
        }

        // 指数退避策略：delay * 2^retry_count，最大不超过10分钟
        let exponential_delay = self.delay_seconds * (2_u64.pow(retry_count.min(6)));
        exponential_delay.min(600) // 最大10分钟
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_parsing() {
        let handler = JavaErrorHandler::new();

        // 测试OOM异常
        let oom_error = handler.parse_error("java.lang.OutOfMemoryError: Java heap space");
        assert!(oom_error.is_some());
        assert_eq!(oom_error.unwrap().error_type, JavaErrorType::OutOfMemory);

        // 测试网络异常
        let network_error = handler.parse_error("java.net.ConnectException: Connection refused");
        assert!(network_error.is_some());
        assert_eq!(network_error.unwrap().error_type, JavaErrorType::NetworkError);

        // 测试OSS异常
        let oss_error = handler.parse_error("com.aliyun.oss.OSSException: Access denied");
        assert!(oss_error.is_some());
        assert_eq!(oss_error.unwrap().error_type, JavaErrorType::StorageConnectionError);
    }

    #[test]
    fn test_retry_strategy() {
        let handler = JavaErrorHandler::new();
        
        let network_error = JavaError::new(
            JavaErrorType::NetworkError,
            "Connection timeout".to_string(),
        );
        
        let strategy = handler.get_retry_strategy(&network_error);
        assert!(strategy.should_retry);
        assert!(strategy.exponential_backoff);
        
        // 测试指数退避
        assert_eq!(strategy.calculate_delay(0), 30);
        assert_eq!(strategy.calculate_delay(1), 60);
        assert_eq!(strategy.calculate_delay(2), 120);
    }

    #[test]
    fn test_non_retryable_errors() {
        let oom_error = JavaError::new(
            JavaErrorType::OutOfMemory,
            "Out of memory".to_string(),
        );
        
        assert!(!oom_error.is_retryable());
        assert_eq!(oom_error.max_retries(), 0);
    }
}
