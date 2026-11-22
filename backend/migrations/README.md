# 初始结构

`src/models/tables.py` 定义初始关系结构。API 启动时显式创建演示库的表；此版本只包含初始 schema。需要在已有部署升级表结构时，应单独引入版本化迁移。

`business_runs.months` 与 `business_runs.waiting_for` 由 `ensure_columns` 在启动时按需补列，用于两岗并发调度；已有演示卷无需手工迁移。正式部署仍应改为版本化迁移。
