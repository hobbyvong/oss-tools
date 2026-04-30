package com.oss.migration;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalTime;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * OSS 迁移工具主程序
 * 
 * 使用方法:
 * 1. 修改配置文件 application.properties (位于 src/main/resources 目录)
 * 2. 编译：mvn clean package -DskipTests
 * 3. 运行：java -jar target/oss-migration-tool-1.0.0-jar-with-dependencies.jar
 * 
 * 或者直接运行：java -cp src/main/java com.oss.migration.OssMigrationApplication
 * 
 * 定时任务说明:
 * - 每天 22:00 自动启动迁移任务
 * - 每个批次执行前都会检查时间窗口 (22:00-07:00)
 * - 如果不在时间窗口内，暂停迁移，等待下次定时任务启动
 */
@Slf4j
public class OssMigrationApplication {
    
    private static MigrationConfig config;
    private static ScheduledExecutorService scheduler;
    
    public static void main(String[] args) {
        log.info("========================================");
        log.info("   阿里云 OSS -> 华为云 OBS 数据迁移工具");
        log.info("========================================");
        
        try {
            // 加载配置
            config = loadConfig(args);
            
            // 验证配置
            validateConfig(config);
            
            // 启动定时任务调度器
            startScheduler();
            
            // 保持主线程运行
            log.info("定时任务调度器已启动，每天 22:00 自动执行迁移任务");
            log.info("按 Ctrl+C 退出程序");
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("正在关闭定时任务调度器...");
                if (scheduler != null) {
                    scheduler.shutdown();
                    try {
                        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                            scheduler.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        scheduler.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                }
                log.info("程序已退出");
            }));
            
        } catch (IllegalArgumentException e) {
            log.error("配置错误：{}", e.getMessage());
            log.error("请检查配置文件 application.properties");
            System.exit(1);
        } catch (Exception e) {
            log.error("启动过程中发生严重错误", e);
            System.exit(1);
        }
    }
    
    /**
     * 启动定时任务调度器
     */
    private static void startScheduler() {
        scheduler = Executors.newScheduledThreadPool(1);
        
        // 计算到下一个 22:00 的延迟时间
        long initialDelay = calculateInitialDelay();
        
        // 每 24 小时执行一次（每天 22:00）
        scheduler.scheduleAtFixedRate(
            () -> {
                try {
                    log.info("========== 定时任务触发：开始执行迁移任务 ==========");
                    executeMigration();
                } catch (Exception e) {
                    log.error("定时任务执行失败", e);
                }
            },
            initialDelay,
            TimeUnit.HOURS.toMillis(24),
            TimeUnit.MILLISECONDS
        );
        
        log.info("定时任务已调度，将在 {} 毫秒后首次执行（预计今天 22:00）", initialDelay);
    }
    
    /**
     * 计算到下一个 22:00 的延迟时间（毫秒）
     */
    private static long calculateInitialDelay() {
        LocalTime now = LocalTime.now();
        LocalTime scheduledTime = LocalTime.of(22, 0);
        
        long delay;
        if (now.isBefore(scheduledTime)) {
            // 当前时间在 22:00 之前，今天 22:00 执行
            delay = java.time.Duration.between(now, scheduledTime).toMillis();
        } else {
            // 当前时间在 22:00 之后，明天 22:00 执行
            delay = java.time.Duration.between(now, scheduledTime).toMillis() 
                  + TimeUnit.DAYS.toMillis(1);
        }
        
        return delay;
    }
    
    /**
     * 执行迁移任务
     */
    private static void executeMigration() {
        OssMigrationService migrationService = null;
        
        try {
            // 创建迁移服务
            migrationService = new OssMigrationService(config);
            
            // 执行迁移
            migrationService.migrate();
            
            log.info("迁移任务完成!");
            
        } catch (Exception e) {
            log.error("迁移过程中发生严重错误", e);
        } finally {
            // 关闭资源
            if (migrationService != null) {
                try {
                    migrationService.close();
                } catch (Exception e) {
                    log.error("关闭资源时出错", e);
                }
            }
        }
    }
    
    /**
     * 加载配置
     */
    private static MigrationConfig loadConfig(String[] args) throws IOException {
        MigrationConfig config = new MigrationConfig();
        
        // 默认配置文件路径
        String configPath = "application.properties";
        
        // 支持通过命令行参数指定配置文件路径
        if (args.length > 0) {
            configPath = args[0];
            log.info("使用指定的配置文件：{}", configPath);
        }
        
        Properties props = new Properties();
        
        // 尝试从类路径加载
        InputStream inputStream = OssMigrationApplication.class.getClassLoader()
            .getResourceAsStream("application.properties");
        
        if (inputStream == null) {
            // 尝试从文件系统加载
            try {
                inputStream = new FileInputStream(configPath);
                log.info("从文件系统加载配置文件：{}", configPath);
            } catch (IOException e) {
                log.warn("未找到配置文件，将使用默认配置");
                log.warn("配置文件示例位置：src/main/resources/application.properties");
                return config;
            }
        } else {
            log.info("从类路径加载配置文件");
        }
        
        try {
            props.load(inputStream);
            
            // 数据库配置
            config.setDbType(getProperty(props, "db.type", "mysql"));
            config.setJdbcUrl(getProperty(props, "db.url", config.getJdbcUrl()));
            config.setDbUsername(getProperty(props, "db.username", config.getDbUsername()));
            config.setDbPassword(getProperty(props, "db.password", config.getDbPassword()));
            config.setTableName(getProperty(props, "db.table.name", "file"));
            config.setSchemaName(getProperty(props, "db.schema.name", "net_disk"));
            
            // 阿里云 OSS 配置
            config.setAliyunEndpoint(getProperty(props, "aliyun.endpoint", config.getAliyunEndpoint()));
            config.setAliyunAccessKeyId(getProperty(props, "aliyun.access.key.id", ""));
            config.setAliyunAccessKeySecret(getProperty(props, "aliyun.access.key.secret", ""));
            config.setAliyunBucketName(getProperty(props, "aliyun.bucket.name", ""));
            
            // 华为云 OBS 配置
            config.setHuaweiEndpoint(getProperty(props, "huawei.endpoint", config.getHuaweiEndpoint()));
            config.setHuaweiAccessKeyId(getProperty(props, "huawei.access.key.id", ""));
            config.setHuaweiSecretAccessKey(getProperty(props, "huawei.secret.access.key", ""));
            config.setHuaweiBucketName(getProperty(props, "huawei.bucket.name", ""));
            
            // 迁移配置
            config.setBatchSize(Integer.parseInt(getProperty(props, "migration.batch.size", "100")));
            config.setThreadCount(Integer.parseInt(getProperty(props, "migration.thread.count", "5")));
            config.setOnlyMigrateValid(Boolean.parseBoolean(getProperty(props, "migration.only.valid", "true")));
            config.setDeleteAfterMigrate(Boolean.parseBoolean(getProperty(props, "migration.delete.after", "false")));
            config.setSourceStorageType(Integer.parseInt(getProperty(props, "migration.source.type", "1")));
            config.setTargetStorageType(Integer.parseInt(getProperty(props, "migration.target.type", "2")));
            
            // 时间窗口配置
            config.setEnableTimeWindow(Boolean.parseBoolean(getProperty(props, "migration.time.window.enabled", "true")));
            config.setTimeWindowStartHour(Integer.parseInt(getProperty(props, "migration.time.window.start.hour", "22")));
            config.setTimeWindowEndHour(Integer.parseInt(getProperty(props, "migration.time.window.end.hour", "7")));
            
        } finally {
            inputStream.close();
        }
        
        return config;
    }
    
    /**
     * 获取配置属性值
     */
    private static String getProperty(Properties props, String key, String defaultValue) {
        String value = props.getProperty(key);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : defaultValue;
    }
    
    /**
     * 验证配置
     */
    private static void validateConfig(MigrationConfig config) {
        StringBuilder errors = new StringBuilder();
        
        // 验证数据库配置
        if (config.getJdbcUrl() == null || config.getJdbcUrl().isEmpty()) {
            errors.append("数据库 URL 不能为空\n");
        }
        
        // 验证阿里云配置
        if (config.getAliyunAccessKeyId() == null || config.getAliyunAccessKeyId().isEmpty()) {
            errors.append("阿里云 AccessKey ID 不能为空\n");
        }
        if (config.getAliyunAccessKeySecret() == null || config.getAliyunAccessKeySecret().isEmpty()) {
            errors.append("阿里云 AccessKey Secret 不能为空\n");
        }
        if (config.getAliyunBucketName() == null || config.getAliyunBucketName().isEmpty()) {
            errors.append("阿里云 Bucket 名称不能为空\n");
        }
        
        // 验证华为云配置
        if (config.getHuaweiAccessKeyId() == null || config.getHuaweiAccessKeyId().isEmpty()) {
            errors.append("华为云 AccessKey ID 不能为空\n");
        }
        if (config.getHuaweiSecretAccessKey() == null || config.getHuaweiSecretAccessKey().isEmpty()) {
            errors.append("华为云 Secret Access Key 不能为空\n");
        }
        if (config.getHuaweiBucketName() == null || config.getHuaweiBucketName().isEmpty()) {
            errors.append("华为云 Bucket 名称不能为空\n");
        }
        
        // 验证迁移配置
        if (config.getBatchSize() <= 0) {
            errors.append("批次大小必须大于 0\n");
        }
        if (config.getThreadCount() <= 0) {
            errors.append("并发线程数必须大于 0\n");
        }
        
        if (errors.length() > 0) {
            throw new IllegalArgumentException(errors.toString());
        }
    }
}
