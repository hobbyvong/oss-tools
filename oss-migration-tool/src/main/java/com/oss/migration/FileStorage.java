package com.oss.migration;

import lombok.Data;

/**
 * 文件存储记录实体类
 */
@Data
public class FileStorage {
    /**
     * 文件 ID
     */
    private String fileId;
    
    /**
     * 创建时间
     */
    private String createTime;
    
    /**
     * 创建用户 ID
     */
    private String createUserId;
    
    /**
     * 文件大小 (字节)
     */
    private Long fileSize;
    
    /**
     * 文件状态 (0-失效，1-生效)
     */
    private Integer fileStatus;
    
    /**
     * 文件 URL(阿里云 OSS 中的 object key)
     */
    private String fileUrl;
    
    /**
     * MD5 唯一标识
     */
    private String identifier;
    
    /**
     * 修改时间
     */
    private String modifyTime;
    
    /**
     * 修改用户 ID
     */
    private String modifyUserId;
    
    /**
     * 存储类型 (1-阿里云，2-华为云)
     */
    private Integer storageType;
}
