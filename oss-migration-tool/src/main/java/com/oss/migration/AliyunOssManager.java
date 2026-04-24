package com.oss.migration;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;

/**
 * 阿里云 OSS 客户端管理器
 */
@Slf4j
public class AliyunOssManager {
    
    private final OSS ossClient;
    private final MigrationConfig config;
    
    public AliyunOssManager(MigrationConfig config) {
        this.config = config;
        this.ossClient = new OSSClientBuilder().build(
            config.getAliyunEndpoint(),
            config.getAliyunAccessKeyId(),
            config.getAliyunAccessKeySecret()
        );
        log.info("阿里云 OSS 客户端初始化成功，Endpoint: {}", config.getAliyunEndpoint());
    }
    
    /**
     * 从阿里云 OSS 下载文件流
     * @param objectKey 对象键 (fileUrl)
     * @return 输入流
     */
    public InputStream downloadFile(String objectKey) {
        try {
            log.debug("从阿里云 OSS 下载文件：{}", objectKey);
            OSSObject ossObject = ossClient.getObject(new GetObjectRequest(
                config.getAliyunBucketName(), objectKey));
            return ossObject.getObjectContent();
        } catch (Exception e) {
            log.error("从阿里云 OSS 下载文件失败：{}, error: {}", objectKey, e.getMessage(), e);
            throw new RuntimeException("下载文件失败：" + objectKey, e);
        }
    }
    
    /**
     * 检查文件是否存在
     * @param objectKey 对象键
     * @return 是否存在
     */
    public boolean doesObjectExist(String objectKey) {
        return ossClient.doesObjectExist(config.getAliyunBucketName(), objectKey);
    }
    
    /**
     * 删除文件 (可选操作)
     * @param objectKey 对象键
     */
    public void deleteFile(String objectKey) {
        try {
            log.info("删除阿里云 OSS 文件：{}", objectKey);
            ossClient.deleteObject(config.getAliyunBucketName(), objectKey);
        } catch (Exception e) {
            log.error("删除阿里云 OSS 文件失败：{}, error: {}", objectKey, e.getMessage(), e);
            throw new RuntimeException("删除文件失败：" + objectKey, e);
        }
    }
    
    /**
     * 关闭客户端
     */
    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
            log.info("阿里云 OSS 客户端已关闭");
        }
    }
}
