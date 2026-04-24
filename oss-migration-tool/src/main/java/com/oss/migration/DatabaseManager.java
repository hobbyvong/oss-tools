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
                "SELECT file_id, create_time, create_user_id, file_size, file_status, " +
                "file_url, identifier, modify_time, modify_user_id, storage_type " +
                "FROM %s.%s WHERE file_status = ? AND storage_type = ? " +
                "ORDER BY create_time LIMIT ? OFFSET ?",
                config.getSchemaName(), config.getTableName()
            );
        } else {
            sql = String.format(
                "SELECT file_id, create_time, create_user_id, file_size, file_status, " +
                "file_url, identifier, modify_time, modify_user_id, storage_type " +
                "FROM %s WHERE file_status = ? AND storage_type = ? " +
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
                    file.setFileId(rs.getString("file_id"));
                    file.setCreateTime(rs.getString("create_time"));
                    file.setCreateUserId(rs.getString("create_user_id"));
                    file.setFileSize(rs.getLong("file_size"));
                    file.setFileStatus(rs.getInt("file_status"));
                    file.setFileUrl(rs.getString("file_url"));
                    file.setIdentifier(rs.getString("identifier"));
                    file.setModifyTime(rs.getString("modify_time"));
                    file.setModifyUserId(rs.getString("modify_user_id"));
                    file.setStorageType(rs.getInt("storage_type"));
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
                "SELECT COUNT(*) FROM %s.%s WHERE file_status = ? AND storage_type = ?",
                config.getSchemaName(), config.getTableName()
            );
        } else {
            sql = String.format(
                "SELECT COUNT(*) FROM %s WHERE file_status = ? AND storage_type = ?",
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
            "UPDATE %s SET storage_type = ?, modify_time = ? WHERE file_id = ?",
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
