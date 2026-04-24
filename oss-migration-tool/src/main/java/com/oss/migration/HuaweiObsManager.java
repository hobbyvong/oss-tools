package com.oss.migration;

import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import com.obs.services.model.PutObjectRequest;
import com.obs.services.model.ObjectMetadata;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;

/**
 * 华为云 OBS 客户端管理器
 */
@Slf4j
public class HuaweiObsManager {
    
    private final ObsClient obsClient;
    private final MigrationConfig config;
    
    public HuaweiObsManager(MigrationConfig config) {
        this.config = config;
        
        // 创建配置对象
        ObsConfiguration obsConfig = new ObsConfiguration();
        obsConfig.setEndPoint(config.getHuaweiEndpoint());
        
        // 使用 AK/SK 初始化客户端
        this.obsClient = new ObsClient(
            config.getHuaweiAccessKeyId(),
            config.getHuaweiSecretAccessKey(),
            obsConfig
        );
        
        log.info("华为云 OBS 客户端初始化成功，Endpoint: {}", config.getHuaweiEndpoint());
    }
    
    /**
     * 上传文件到华为云 OBS
     * @param objectKey 对象键
     * @param inputStream 输入流
     * @param fileSize 文件大小
     */
    public void uploadFile(String objectKey, InputStream inputStream, long fileSize) {
        try {
            log.debug("上传文件到华为云 OBS: {}, size: {} bytes", objectKey, fileSize);
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(fileSize);
            
            PutObjectRequest request = new PutObjectRequest();
            request.setBucketName(config.getHuaweiBucketName());
            request.setObjectKey(objectKey);
            request.setInput(inputStream);
            request.setMetadata(metadata);
            
            obsClient.putObject(request);
            log.debug("文件上传成功：{}", objectKey);
        } catch (Exception e) {
            log.error("上传文件到华为云 OBS 失败：{}, error: {}", objectKey, e.getMessage(), e);
            throw new RuntimeException("上传文件失败：" + objectKey, e);
        }
    }
    
    /**
     * 检查文件是否存在
     * @param objectKey 对象键
     * @return 是否存在
     */
    public boolean doesObjectExist(String objectKey) {
        try {
            return obsClient.doesObjectExist(config.getHuaweiBucketName(), objectKey);
        } catch (Exception e) {
            log.error("检查文件存在性失败：{}, error: {}", objectKey, e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 关闭客户端
     */
    public void close() {
        if (obsClient != null) {
            try {
                obsClient.close();
                log.info("华为云 OBS 客户端已关闭");
            } catch (Exception e) {
                log.error("关闭华为云 OBS 客户端时出错", e);
            }
        }
    }
}
