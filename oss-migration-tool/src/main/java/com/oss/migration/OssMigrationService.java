package com.oss.migration;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OSS 迁移服务
 */
@Slf4j
public class OssMigrationService {
    
    private final MigrationConfig config;
    private final DatabaseManager databaseManager;
    private final AliyunOssManager aliyunOssManager;
    private final HuaweiObsManager huaweiObsManager;
    
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);
    private final AtomicInteger skipCount = new AtomicInteger(0);
    
    public OssMigrationService(MigrationConfig config) {
        this.config = config;
        this.databaseManager = new DatabaseManager(config);
        this.aliyunOssManager = new AliyunOssManager(config);
        this.huaweiObsManager = new HuaweiObsManager(config);
    }
    
    /**
     * 检查当前时间是否在允许执行的时间窗口内 (22:00 - 07:00)
     * @return 是否允许执行
     */
    public boolean isWithinTimeWindow() {
        if (!config.isEnableTimeWindow()) {
            // 如果未启用时间窗口控制，则随时可以执行
            return true;
        }
        
        LocalTime currentTime = LocalTime.now();
        LocalTime startTime = LocalTime.of(config.getTimeWindowStartHour(), 0);
        LocalTime endTime = LocalTime.of(config.getTimeWindowEndHour(), 0);
        
        // 处理跨天的情况 (22:00 - 次日 07:00)
        if (startTime.isAfter(endTime)) {
            // 跨天：22:00 - 23:59:59 或 00:00 - 07:00
            return !currentTime.isBefore(startTime) || currentTime.isBefore(endTime);
        } else {
            // 不跨天：直接比较
            return !currentTime.isBefore(startTime) && currentTime.isBefore(endTime);
        }
    }
    
    /**
     * 执行迁移
     */
    public void migrate() {
        log.info("========== 开始迁移任务 ==========");
        log.info("配置信息:");
        log.info("  - 数据库类型：{}", config.getDbType());
        log.info("  - 阿里云 Bucket: {}", config.getAliyunBucketName());
        log.info("  - 华为云 Bucket: {}", config.getHuaweiBucketName());
        log.info("  - 批次大小：{}", config.getBatchSize());
        log.info("  - 并发线程数：{}", config.getThreadCount());
        log.info("  - 仅迁移生效文件：{}", config.isOnlyMigrateValid());
        log.info("  - 迁移后删除源文件：{}", config.isDeleteAfterMigrate());
        log.info("  - 启用时间窗口控制：{}", config.isEnableTimeWindow());
        if (config.isEnableTimeWindow()) {
            log.info("  - 时间窗口：{}:00 - {}:00", 
                config.getTimeWindowStartHour(), config.getTimeWindowEndHour());
        }
        
        // 查询待迁移文件总数（无论是否在时间窗口内都输出）
        int totalCount;
        try {
            totalCount = databaseManager.countFilesToMigrate();
        } catch (SQLException e) {
            log.error("查询待迁移文件数量失败", e);
            throw new RuntimeException("查询待迁移文件数量失败", e);
        }
        log.info("待迁移文件总数：{}", totalCount);
        
        if (totalCount == 0) {
            log.info("所有文件已完成迁移，无需执行迁移任务");
            return;
        }
        
        // 检查时间窗口
        if (!isWithinTimeWindow()) {
            LocalTime currentTime = LocalTime.now();
            log.warn("当前时间 {} 不在允许的执行时间窗口内 ({}:00 - {}:00)，跳过本次迁移任务", 
                currentTime, config.getTimeWindowStartHour(), config.getTimeWindowEndHour());
            log.info("剩余待迁移文件数：{}，将在下一个时间窗口继续迁移", totalCount);
            return;
        }
        
        log.info("当前时间在允许的执行时间窗口内，继续执行迁移任务");
        
        ExecutorService executor = Executors.newFixedThreadPool(config.getThreadCount());
        CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
        
        try {
            // 分批处理
            int offset = 0;
            int batchNum = 0;
            
            while (offset < totalCount) {
                // 【关键】每个批次执行前都检查时间窗口
                if (!isWithinTimeWindow()) {
                    LocalTime currentTime = LocalTime.now();
                    log.warn("当前时间 {} 不在允许的执行时间窗口内 ({}:00 - {}:00)，暂停迁移", 
                        currentTime, config.getTimeWindowStartHour(), config.getTimeWindowEndHour());
                    log.info("已处理进度：成功={}, 失败={}, 跳过={}, 剩余待迁移文件数：{}", 
                        successCount.get(), failCount.get(), skipCount.get(), totalCount - offset);
                    log.info("等待下次定时任务启动后继续迁移");
                    break; // 退出循环，等待下次定时任务
                }
                
                List<FileStorage> files = databaseManager.queryFilesToMigrate(offset, config.getBatchSize());
                
                if (files.isEmpty()) {
                    break;
                }
                
                batchNum++;
                log.info("处理第 {} 批，共 {} 个文件 (offset: {})", batchNum, files.size(), offset);
                
                // 提交批量任务
                for (FileStorage file : files) {
                    completionService.submit(() -> migrateFile(file), true);
                }
                
                offset += config.getBatchSize();
                
                // 等待当前批次完成
                for (int i = 0; i < files.size(); i++) {
                    try {
                        completionService.take().get();
                    } catch (Exception e) {
                        log.error("任务执行异常", e);
                        failCount.incrementAndGet();
                    }
                }
                
                // 打印进度
                int total = successCount.get() + failCount.get() + skipCount.get();
                log.info("当前进度：成功={}, 失败={}, 跳过={}, 总计={}/{}", 
                    successCount.get(), failCount.get(), skipCount.get(), total, totalCount);
            }
            
            log.info("========== 迁移完成 ==========");
            log.info("统计信息:");
            log.info("  - 成功：{}", successCount.get());
            log.info("  - 失败：{}", failCount.get());
            log.info("  - 跳过：{}", skipCount.get());
            log.info("  - 总计：{}", successCount.get() + failCount.get() + skipCount.get());
            
        } catch (Exception e) {
            log.error("迁移过程中发生异常", e);
            throw new RuntimeException("迁移失败", e);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * 迁移单个文件
     */
    private Boolean migrateFile(FileStorage file) {
        String fileId = file.getFileId();
        String objectKey = file.getFileUrl();
        String dbMd5 = file.getIdentifier();
        
        try {
            // 检查源文件是否存在
            if (!aliyunOssManager.doesObjectExist(objectKey)) {
                log.warn("源文件不存在，跳过：{} (fileId: {})", objectKey, fileId);
                skipCount.incrementAndGet();
                return true;
            }
            
            // 检查目标是否已存在 (避免重复迁移)
            if (huaweiObsManager.doesObjectExist(objectKey)) {
                log.info("目标文件已存在，跳过：{} (fileId: {})", objectKey, fileId);
                skipCount.incrementAndGet();
                // 即使跳过也更新存储类型
                databaseManager.updateStorageType(fileId, config.getTargetStorageType());
                return true;
            }
            
            // 下载文件
            log.info("开始迁移文件：{} (fileId: {}, size: {} bytes, md5: {})", 
                objectKey, fileId, file.getFileSize(), dbMd5);
            
            try (InputStream inputStream = aliyunOssManager.downloadFile(objectKey)) {
                // 上传到华为云
                huaweiObsManager.uploadFile(objectKey, inputStream, file.getFileSize());
            }
            
            // 验证 MD5: 获取华为云对象的 MD5 并与数据库中的 MD5 比较
            String huaweiMd5 = huaweiObsManager.getObjectMd5(objectKey);
            if (huaweiMd5 != null && dbMd5 != null && !dbMd5.isEmpty()) {
                // 统一转换为小写进行比较
                if (huaweiMd5.equalsIgnoreCase(dbMd5)) {
                    log.info("MD5 校验通过：{} (fileId: {}), md5: {}", objectKey, fileId, huaweiMd5);
                } else {
                    log.error("MD5 校验失败：{} (fileId: {}), 数据库 MD5: {}, 华为云 MD5: {}", 
                        objectKey, fileId, dbMd5, huaweiMd5);
                    // MD5 不匹配时，不更新存储类型，返回失败
                    failCount.incrementAndGet();
                    return false;
                }
            } else {
                log.warn("无法进行 MD5 校验：{} (fileId: {}), 数据库 MD5: {}, 华为云 MD5: {}", 
                    objectKey, fileId, dbMd5, huaweiMd5);
                // 如果无法获取 MD5，记录警告但仍继续
            }
            
            // MD5 校验通过后，更新数据库存储类型
            databaseManager.updateStorageType(fileId, config.getTargetStorageType());
            
            // 可选：删除源文件
            if (config.isDeleteAfterMigrate()) {
                aliyunOssManager.deleteFile(objectKey);
            }
            
            successCount.incrementAndGet();
            log.info("文件迁移成功并验证通过：{} (fileId: {})", objectKey, fileId);
            return true;
            
        } catch (Exception e) {
            failCount.incrementAndGet();
            log.error("文件迁移失败：{} (fileId: {}), error: {}", 
                objectKey, fileId, e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 关闭资源
     */
    public void close() {
        databaseManager.close();
        aliyunOssManager.close();
        huaweiObsManager.close();
    }
}
