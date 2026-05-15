package com.oss.migration;

import lombok.Data;

/**
 * 迁移配置类
 */
@Data
public class MigrationConfig {
    
    // ==================== 数据库配置 ====================
    /**
     * 数据库类型：mysql 或 postgresql
     */
    private String dbType = "mysql";
    
    /**
     * 数据库 JDBC URL
     */
    private String jdbcUrl = "jdbc:mysql://localhost:3306/net_disk?useSSL=false&serverTimezone=UTC&characterEncoding=utf8";
    
    /**
     * 数据库用户名
     */
    private String dbUsername = "root";
    
    /**
     * 数据库密码
     */
    private String dbPassword = "password";
    
    /**
     * 文件表名
     */
    private String tableName = "file";
    
    /**
     * Schema 名称 (PostgreSQL/openGauss 使用)
     */
    private String schemaName = "net_disk";
    
    // ==================== 阿里云 OSS 配置 ====================
    /**
     * 阿里云 OSS Endpoint
     */
    private String aliyunEndpoint = "oss-cn-hangzhou.aliyuncs.com";
    
    /**
     * 阿里云 AccessKey ID
     */
    private String aliyunAccessKeyId = "";
    
    /**
     * 阿里云 AccessKey Secret
     */
    private String aliyunAccessKeySecret = "";
    
    /**
     * 阿里云 OSS Bucket 名称
     */
    private String aliyunBucketName = "";
    
    // ==================== 华为云 OBS 配置 ====================
    /**
     * 华为云 OBS Endpoint
     */
    private String huaweiEndpoint = "obs.cn-north-4.myhuaweicloud.com";
    
    /**
     * 华为云 AccessKey ID
     */
    private String huaweiAccessKeyId = "";
    
    /**
     * 华为云 Secret Access Key
     */
    private String huaweiSecretAccessKey = "";
    
    /**
     * 华为云 OBS Bucket 名称
     */
    private String huaweiBucketName = "";
    
    // ==================== 迁移配置 ====================
    /**
     * 每次查询的记录数
     */
    private int batchSize = 100;
    
    /**
     * 并发线程数
     */
    private int threadCount = 5;
    
    /**
     * 仅迁移生效的文件 (fileStatus=1)
     */
    private boolean onlyMigrateValid = true;
    
    /**
     * 迁移后是否删除阿里云上的文件 (默认 false)
     */
    private boolean deleteAfterMigrate = false;
    
    /**
     * 源存储类型标识 (数据库中当前存储类型)
     */
    private int sourceStorageType = 1;
    
    /**
     * 目标存储类型标识 (数据库中迁移后的存储类型)
     */
    private int targetStorageType = 2;
    
    // ==================== 定时任务配置 ====================
    /**
     * 是否启用时间窗口控制 (true: 只在指定时间段执行，false: 随时执行)
     */
    private boolean enableTimeWindow = true;
    
    /**
     * 时间窗口开始小时 (22:00)
     */
    private int timeWindowStartHour = 22;
    
    /**
     * 时间窗口结束小时 (07:00)
     */
    private int timeWindowEndHour = 7;
    
    /**
     * 定时任务触发时间 - 小时 (24 小时制，默认 22 点)
     */
    private int scheduledTaskHour = 22;
    
    /**
     * 定时任务触发时间 - 分钟 (0-59，默认 0 分)
     */
    private int scheduledTaskMinute = 0;
}
