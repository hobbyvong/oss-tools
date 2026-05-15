# 阿里云 OSS 到华为云 OBS 数据迁移工具

## 项目简介

本工具用于将工作网盘系统中的文件数据从阿里云对象存储 (OSS) 迁移到华为云对象存储 (OBS)，
同时更新数据库中文件存储记录的存储类型字段。

## 技术栈

- **Java 17**
- **Maven 3.8+**
- **阿里云 OSS SDK 3.17.2**
- **华为云 OBS SDK 3.23.9**
- **HikariCP 连接池**
- **支持 MySQL/PostgreSQL/openGauss 数据库**

## 目录结构

```
oss-migration-tool/
├── src/main/java/com/oss/migration/
│   ├── OssMigrationApplication.java    # 主程序入口
│   ├── MigrationConfig.java            # 配置类
│   ├── FileStorage.java                # 文件存储实体类
│   ├── DatabaseManager.java            # 数据库操作类
│   ├── AliyunOssManager.java           # 阿里云 OSS 客户端
│   ├── HuaweiObsManager.java           # 华为云 OBS 客户端
│   └── OssMigrationService.java        # 迁移服务核心逻辑
├── src/main/resources/
│   ├── application.properties          # 配置文件
│   └── logback.xml                     # 日志配置
├── pom.xml                             # Maven 配置
└── README.md                           # 说明文档
```

## 快速开始

### 1. 环境要求

- JDK 17 或更高版本
- Maven 3.8 或更高版本

### 2. 配置修改

编辑 `src/main/resources/application.properties` 文件，填入您的实际配置：

```properties
# ==================== 数据库配置 ====================
# 数据库类型：mysql 或 postgresql (openGauss 使用 postgresql)
db.type = mysql

# MySQL 示例
db.url = jdbc:mysql://your-db-host:3306/net_disk?useSSL=false&serverTimezone=UTC&characterEncoding=utf8

# PostgreSQL/openGauss 示例
# db.url = jdbc:postgresql://your-db-host:5432/net_disk

db.username = your_db_username
db.password = your_db_password

# 文件表名
db.table.name = file

# Schema 名称 (PostgreSQL/openGauss 使用)
db.schema.name = net_disk

# ==================== 阿里云 OSS 配置 ====================
aliyun.endpoint = oss-cn-hangzhou.aliyuncs.com
aliyun.access.key.id = your_aliyun_ak
aliyun.access.key.secret = your_aliyun_sk
aliyun.bucket.name = your-aliyun-bucket

# ==================== 华为云 OBS 配置 ====================
huawei.endpoint = obs.cn-north-4.myhuaweicloud.com
huawei.access.key.id = your_huawei_ak
huawei.secret.access.key = your_huawei_sk
huawei.bucket.name = your-huawei-bucket

# ==================== 迁移配置 ====================
migration.batch.size = 100          # 每批次处理的记录数
migration.thread.count = 5          # 并发线程数
migration.only.valid = true         # 仅迁移生效的文件 (fileStatus=1)
migration.delete.after = false      # 迁移后是否删除源文件 (建议先设为 false)
migration.source.type = 1           # 源存储类型标识
migration.target.type = 2           # 目标存储类型标识

# ==================== 时间窗口配置 ====================
# 是否启用时间窗口控制 (true: 只在指定时间段执行，false: 随时执行)
migration.time.window.enabled = true

# 时间窗口开始小时 (24 小时制，默认 22 点)
migration.time.window.start.hour = 22

# 时间窗口结束小时 (24 小时制，默认 7 点)
migration.time.window.end.hour = 7
```

### 3. 编译打包

```bash
cd oss-migration-tool
mvn clean package -DskipTests
```

编译成功后，在 `target` 目录下会生成：
- `oss-migration-tool-1.0.0.jar` - 不含依赖的 jar 包
- `oss-migration-tool-1.0.0-jar-with-dependencies.jar` - 包含所有依赖的可执行 jar 包

### 4. 运行迁移

```bash
# 使用默认配置文件 (src/main/resources/application.properties)
java -jar target/oss-migration-tool-1.0.0-jar-with-dependencies.jar

# 或指定外部配置文件
java -jar target/oss-migration-tool-1.0.0-jar-with-dependencies.jar /path/to/your/application.properties
```

## 数据库表结构

本工具基于以下表结构设计（驼峰命名）：

```sql
CREATE TABLE "net_disk"."file" (
  "fileId" varchar(20) NOT NULL,
  "createTime" varchar(25),
  "createUserId" varchar(32),
  "fileSize" int8,
  "fileStatus" int4,           -- 文件状态 (0-失效，1-生效)
  "fileUrl" varchar(500),      -- 文件在对象存储中的 key
  "identifier" varchar(200),   -- MD5 唯一标识
  "modifyTime" varchar(25),
  "modifyUserId" varchar(32),
  "storageType" int4           -- 存储类型 (1-阿里云，2-华为云)
);
```

## 迁移流程

1. **读取配置** - 从配置文件加载数据库和 OSS 配置
2. **时间窗口检查** - 如果启用了时间窗口控制，检查当前时间是否在允许范围内 (默认 22:00-07:00)
3. **查询待迁移文件** - 根据 `fileStatus` 和 `storageType` 筛选需要迁移的文件，并输出剩余文件数量
4. **批量迁移** - 并发处理文件迁移：
   - 从阿里云 OSS 下载文件
   - 上传到华为云 OBS
   - **获取华为云 OBS 对象的 MD5 值**
   - **与数据库中的 `identifier` (MD5) 进行比较验证**
   - **MD5 校验通过后，更新数据库中的 `storageType` 字段**
5. **可选清理** - 如果配置了 `migration.delete.after=true`，删除阿里云上的源文件
6. **输出统计** - 显示成功、失败、跳过的文件数量

## 日志说明

日志文件输出位置：`logs/oss-migration.log`

日志级别配置：
- 迁移业务日志：INFO
- SDK 日志：WARN

## 注意事项

1. **数据安全**
   - 首次运行建议设置 `migration.delete.after = false`
   - 确认迁移完成后再手动清理阿里云上的文件
   - 建议先在测试环境验证

2. **网络要求**
   - 确保运行环境可以访问阿里云 OSS 和华为云 OBS
   - 确保可以连接数据库

3. **性能调优**
   - 根据网络带宽调整 `migration.thread.count`（建议 5-20）
   - 根据内存大小调整 `migration.batch.size`（建议 50-200）

4. **断点续传**
   - 工具会自动跳过已存在于华为云 OBS 的文件
   - 中断后重新运行会继续迁移剩余文件

5. **数据库兼容**
   - MySQL：直接使用
   - PostgreSQL/openGauss：设置 `db.type = postgresql`

## 常见问题

### Q: 迁移过程中断怎么办？
A: 重新运行程序即可，会自动跳过已迁移的文件。

### Q: 如何验证迁移是否成功？
A: 检查日志中的统计信息，并抽样验证华为云 OBS 中的文件。

### Q: 可以只迁移部分文件吗？
A: 可以通过修改 `migration.only.valid` 或在数据库中临时修改 `storageType` 来控制。

### Q: 迁移速度慢怎么办？
A: 
- 增加 `migration.thread.count`（注意不要超过网络带宽承载能力）
- 确保运行环境与 OSS/OBS 在同一地域或有良好网络连接

## 许可证

本项目仅供内部使用。

## 联系方式

如有问题，请联系开发团队。
