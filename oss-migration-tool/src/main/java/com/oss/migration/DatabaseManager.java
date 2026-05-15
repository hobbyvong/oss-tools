package com.oss.migration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据库访问层
 */
@Slf4j
public class DatabaseManager {
    
    private final HikariDataSource dataSource;
    private final MigrationConfig config;
    
    public DatabaseManager(MigrationConfig config) {
        this.config = config;
        this.dataSource = createDataSource(config);
    }
    
    private HikariDataSource createDataSource(MigrationConfig config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setUsername(config.getDbUsername());
        hikariConfig.setPassword(config.getDbPassword());
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        
        // 根据数据库类型设置驱动
        if ("postgresql".equalsIgnoreCase(config.getDbType()) || 
            "opengauss".equalsIgnoreCase(config.getDbType())) {
            hikariConfig.setDriverClassName("org.postgresql.Driver");
        } else {
            hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        }
        
        return new HikariDataSource(hikariConfig);
    }
    
    /**
     * 查询需要迁移的文件列表
     */
    public List<FileStorage> queryFilesToMigrate(int offset, int limit) throws SQLException {
        List<FileStorage> fileList = new ArrayList<>();
        
        String sql;
        if ("postgresql".equalsIgnoreCase(config.getDbType()) || 
            "opengauss".equalsIgnoreCase(config.getDbType())) {
            sql = String.format(
                "SELECT fileId, createTime, createUserId, fileSize, fileStatus, " +
                "fileUrl, identifier, modifyTime, modifyUserId, storageType " +
                "FROM %s.%s WHERE fileStatus = ? AND storageType = ? " +
                "ORDER BY createTime LIMIT ? OFFSET ?",
                config.getSchemaName(), config.getTableName()
            );
        } else {
            sql = String.format(
                "SELECT fileId, createTime, createUserId, fileSize, fileStatus, " +
                "fileUrl, identifier, modifyTime, modifyUserId, storageType " +
                "FROM %s WHERE fileStatus = ? AND storageType = ? " +
                "LIMIT ? OFFSET ?",
                config.getTableName()
            );
        }
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setInt(1, config.isOnlyMigrateValid() ? 1 : 0);
            ps.setInt(2, config.getSourceStorageType());
            ps.setInt(3, limit);
            ps.setInt(4, offset);
            
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    FileStorage file = new FileStorage();
                    file.setFileId(rs.getString("fileId"));
                    file.setCreateTime(rs.getString("createTime"));
                    file.setCreateUserId(rs.getString("createUserId"));
                    file.setFileSize(rs.getLong("fileSize"));
                    file.setFileStatus(rs.getInt("fileStatus"));
                    file.setFileUrl(rs.getString("fileUrl"));
                    file.setIdentifier(rs.getString("identifier"));
                    file.setModifyTime(rs.getString("modifyTime"));
                    file.setModifyUserId(rs.getString("modifyUserId"));
                    file.setStorageType(rs.getInt("storageType"));
                    fileList.add(file);
                }
            }
        }
        
        return fileList;
    }
    
    /**
     * 查询总记录数
     */
    public int countFilesToMigrate() throws SQLException {
        String sql;
        if ("postgresql".equalsIgnoreCase(config.getDbType()) || 
            "opengauss".equalsIgnoreCase(config.getDbType())) {
            sql = String.format(
                "SELECT COUNT(*) FROM %s.%s WHERE fileStatus = ? AND storageType = ?",
                config.getSchemaName(), config.getTableName()
            );
        } else {
            sql = String.format(
                "SELECT COUNT(*) FROM %s WHERE fileStatus = ? AND storageType = ?",
                config.getTableName()
            );
        }
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setInt(1, config.isOnlyMigrateValid() ? 1 : 0);
            ps.setInt(2, config.getSourceStorageType());
            
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        
        return 0;
    }
    
    /**
     * 更新文件存储类型
     */
    public void updateStorageType(String fileId, int newStorageType) throws SQLException {
        String sql = String.format(
            "UPDATE %s SET storageType = ?, modifyTime = ? WHERE fileId = ?",
            "postgresql".equalsIgnoreCase(config.getDbType()) || 
            "opengauss".equalsIgnoreCase(config.getDbType()) ? 
                config.getSchemaName() + "." + config.getTableName() : 
                config.getTableName()
        );
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            
            ps.setInt(1, newStorageType);
            ps.setString(2, String.valueOf(System.currentTimeMillis()));
            ps.setString(3, fileId);
            
            int updated = ps.executeUpdate();
            if (updated == 0) {
                log.warn("未找到文件记录：{}", fileId);
            }
        }
    }
    
    /**
     * 关闭数据源
     */
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }
}
