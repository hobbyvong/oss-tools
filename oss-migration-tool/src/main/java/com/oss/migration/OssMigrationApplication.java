package com.oss.migration;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * OSS 迁移工具主程序
 * 
 * 使用方法:
 * 1. 修改配置文件 application.properties (位于 src/main/resources 目录)
 * 2. 编译：mvn clean package -DskipTests
 * 3. 运行：java -jar target/oss-migration-tool-1.0.0-jar-with-dependencies.jar
 * 
 * 或者直接运行：java -cp src/main/java com.oss.migration.OssMigrationApplication
 */
@Slf4j
public class OssMigrationApplication {
    
    public static void main(String[] args) {
        log.info("========================================");
        log.info("   阿里云 OSS -> 华为云 OBS 数据迁移工具");
        log.info("========================================");
        
        MigrationConfig config = null;
        OssMigrationService migrationService = null;
        
        try {
            // 加载配置
            config = loadConfig(args);
            
            // 验证配置
            validateConfig(config);
            
            // 创建迁移服务
            migrationService = new OssMigrationService(config);
            
            // 执行迁移
            migrationService.migrate();
            
            log.info("迁移任务完成!");
            
        } catch (IllegalArgumentException e) {
            log.error("配置错误：{}", e.getMessage());
            log.error("请检查配置文件 application.properties");
            System.exit(1);
        } catch (Exception e) {
            log.error("迁移过程中发生严重错误", e);
            System.exit(1);
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
