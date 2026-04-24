package com.oss.migration;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
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
        
        ExecutorService executor = Executors.newFixedThreadPool(config.getThreadCount());
        CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
        
        try {
            // 查询总记录数
            int totalCount = databaseManager.countFilesToMigrate();
            log.info("待迁移文件总数：{}", totalCount);
            
            if (totalCount == 0) {
                log.info("没有需要迁移的文件");
                return;
            }
            
            // 分批处理
            int offset = 0;
            int batchNum = 0;
            
            while (offset < totalCount) {
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
            log.info("开始迁移文件：{} (fileId: {}, size: {} bytes)", 
                objectKey, fileId, file.getFileSize());
            
            try (InputStream inputStream = aliyunOssManager.downloadFile(objectKey)) {
                // 上传到华为云
                huaweiObsManager.uploadFile(objectKey, inputStream, file.getFileSize());
            }
            
            // 更新数据库存储类型
            databaseManager.updateStorageType(fileId, config.getTargetStorageType());
            
            // 可选：删除源文件
            if (config.isDeleteAfterMigrate()) {
                aliyunOssManager.deleteFile(objectKey);
            }
            
            successCount.incrementAndGet();
            log.info("文件迁移成功：{} (fileId: {})", objectKey, fileId);
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
