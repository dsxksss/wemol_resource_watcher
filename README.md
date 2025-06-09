# Wemol平台作业模块调度资源监控记录程序

该程序用于监控wemol_rc_task前缀的Docker容器资源使用情况，并将监控数据按模块名分类记录到CSV文件中。程序经过多轮优化，已在生产环境验证可用。

## 功能特性

- 自动发现所有`wemol_rc_task`前缀的Docker容器
- 解析容器名称中的任务ID、作业ID等信息
- 定期获取容器资源使用统计信息（CPU、内存、网络IO、块IO等）
- **完整的GPU监控**：获取nvidia-smi的全部信息，包括温度、风扇、功耗、显存等
- 从对应的`task.json`文件中读取模块名称信息
- **按模块名分类存储**：在`module_resource`目录下按模块名创建子文件夹存储CSV文件
- 支持可配置的监控间隔和日志级别
- 兼容多版本Python环境（已解决subprocess兼容性问题）
- 完善的错误处理机制，确保程序稳定运行
- **精确时间间隔控制**：动态调整等待时间，确保监控间隔准确

## 使用方法

### 基本使用

```bash
# 使用默认配置（5秒间隔，INFO日志级别）
python recorder.py

# 指定监控间隔为10秒
python recorder.py --interval 10

# 设置日志级别为DEBUG
python recorder.py --log-level DEBUG
```

### 命令行参数

- `--interval`: 监控间隔时间（秒），默认5秒
- `--log-level`: 日志级别，可选值：DEBUG, INFO, WARNING, ERROR，默认INFO

## 文件结构

```
项目根目录/
├── module_resource/                    # 按模块分类的资源监控数据
│   ├── MD (GMX2024)/                  # 模块文件夹（自动创建）
│   │   ├── 132178.csv                 # 任务监控数据
│   │   └── ...                        # 其他任务数据
│   ├── Another Module/                # 其他模块文件夹
│   │   └── ...
│   └── ...
├── wemol_resource_monitor.log         # 程序运行日志
└── recorder.py                       # 主程序
```

## 工作原理

### 1. 容器发现

程序通过执行`docker ps --format "{{.ID}} {{.Names}}"`命令获取所有运行中的容器，然后筛选出以`wemol_rc_task`为前缀的容器。

对于容器名称如：`wemol_rc_task_gpu_132178_182060_334177`

程序会解析出：
- `task_id`: 132178（第一个数字，用于构建task.json路径和CSV文件名）
- `job_id`: 182060（第二个数字，记录到CSV中）

注：第三个数字不进行处理，因为在当前监控需求中不需要。

### 2. 资源监控

程序使用`docker stats --no-stream`命令获取每个容器的资源使用情况，包括：
- CPU使用百分比
- 内存使用量和百分比
- 网络IO
- 块IO
- 进程数

### 3. 模块信息获取

根据任务ID构建路径并读取`task.json`文件：

路径规则：`/data/PRG/RCall/Worker.{类型}/work_blob/{倒数四位的前两位}/{倒数两位}/{完整数字}/task.json`

例如任务ID `132178`：
- 倒数两位：`78`
- 倒数四位的前两位：`21`
- 完整路径：`/data/PRG/RCall/Worker.GPU/work_blob/21/78/132178/task.json`

程序会尝试不同的Worker类型（GPU、CPU、AF2、前面都不符合则是ALL）直到找到对应的文件。

### 4. 完整GPU监控

程序通过以下优化步骤监控完整的GPU信息：

1. **获取容器进程**：使用`docker top {container_id}`命令获取容器内运行的所有进程PID
2. **获取GPU进程信息**：使用`nvidia-smi --query-compute-apps=pid,gpu_uuid,used_memory --format=csv,noheader,nounits`命令获取当前GPU上运行的进程信息
3. **获取完整GPU信息**：使用`nvidia-smi --query-gpu=index,name,memory.total,memory.used,utilization.gpu,utilization.memory,temperature.gpu,fan.speed,power.draw,power.limit --format=csv,noheader,nounits`命令获取全部GPU状态信息
4. **PID匹配**：将容器进程PID与GPU进程PID进行匹配
5. **数据汇总**：统计GPU型号、内存使用、利用率、温度、风扇、功耗等完整信息

如果容器未使用GPU，相关字段将显示为"N/A"。

### 5. 按模块分类存储

程序会：
1. 根据任务ID获取模块名称
2. 清理模块名称中的特殊字符，使其适合作为文件夹名
3. 在`module_resource`目录下创建以模块名命名的子文件夹
4. 将每个任务的CSV文件存储在对应的模块文件夹中

### 6. 数据记录

每个任务ID对应一个CSV文件（如`module_resource/MD (GMX2024)/132178.csv`），包含完整的22个字段：

| 序号 | 列名 | 描述 | 示例 |
|------|------|------|------|
| 1 | task_id | 任务ID | 132178 |
| 2 | job_id | 作业ID | 182060 |
| 3 | module_name | 模块名称（从task.json读取） | MD (GMX2024) |
| 4 | timestamp | 记录时间戳 | 2025-06-04 09:44:28 |
| 5 | container | 容器名称 | wemol_rc_task_gpu_132178_182060_334177 |
| 6 | cpu_percent | CPU使用百分比 | 3189.83% |
| 7 | mem_usage | 内存使用量 | 975.7MiB / 250.3GiB |
| 8 | mem_percent | 内存使用百分比 | 0.38% |
| 9 | net_io | 网络IO | 746B / 0B |
| 10 | block_io | 块IO | 1.58MB / 1.71GB |
| 11 | pids | 进程数 | 39 |
| 12 | gpu_count | 使用的GPU数量 | 1 |
| 13 | gpu_ids | 使用的GPU ID列表（逗号分隔） | 0 |
| 14 | gpu_names | GPU型号名称（逗号分隔） | NVIDIA GeForce RTX 3090 |
| 15 | gpu_memory_used | GPU内存使用量（MB，逗号分隔） | 522MB |
| 16 | gpu_memory_total | GPU总内存容量（MB，逗号分隔） | 24576MB |
| 17 | gpu_utilization | GPU利用率（%，逗号分隔） | 88% |
| 18 | gpu_memory_utilization | GPU内存利用率（%，逗号分隔） | 35% |
| 19 | gpu_temperature | GPU温度（°C，逗号分隔） | 72°C |
| 20 | gpu_fan_speed | GPU风扇转速（%，逗号分隔） | 54% |
| 21 | gpu_power_draw | GPU功耗（W，逗号分隔） | 341.52W |
| 22 | gpu_power_limit | GPU功耗限制（W，逗号分隔） | 350.00W |

## 输出文件

### CSV文件
- 文件名：`{task_id}.csv`
- 位置：`module_resource/{模块名}/`
- 编码：UTF-8
- 格式：包含表头的标准CSV格式，共22个字段

### 日志文件
- 文件名：`wemol_resource_monitor.log`
- 位置：程序运行目录
- 内容：程序运行日志，包括错误信息、警告和调试信息

## 技术特性

### Python兼容性
- 支持Python 2.7及以上版本
- 使用`stdout=subprocess.PIPE`和`universal_newlines=True`替代`capture_output`和`text`参数
- 兼容不同版本的subprocess模块

### Docker命令优化
- 使用简单格式`{{.ID}} {{.Names}}`输出容器信息，避免制表符/空格分隔问题
- 使用`--no-stream`参数获取实时资源统计信息

### GPU监控增强
- 改进的进程发现机制，使用`docker top`替代`ps aux`
- 优化的nvidia-smi调用方式，获取完整的GPU状态信息
- 实时GPU利用率、温度、风扇、功耗监控
- 支持多GPU环境和GPU信息聚合

### 时间间隔控制
- 精确计算程序执行时间
- 动态调整等待时间，确保监控间隔准确
- 当执行时间超过设定间隔时发出警告

### 文件管理
- 自动创建目录结构
- 模块名称清理，确保文件夹名称合规
- 支持中文和特殊字符的模块名

## 错误处理

程序具有完善的错误处理机制：

1. **Docker命令执行失败**：记录错误日志并继续监控
2. **容器统计信息获取失败**：跳过该容器并继续监控其他容器
3. **task.json文件读取失败**：模块名称显示为"Unknown"
4. **CSV文件写入失败**：记录错误日志但不影响其他操作
5. **GPU信息获取失败**：GPU相关字段显示为"N/A"
6. **目录创建失败**：记录错误并尝试在当前目录创建文件

## 生产环境部署

### 安全性
- 程序仅包含读取操作和数据追加功能
- 无任何删除或修改已有数据的操作
- 安全可靠，适合生产环境长期运行

### 环境要求
1. 确保Docker已安装且当前用户有权限执行Docker命令
2. 确保能够访问`/data/PRG/RCall/Worker.*`路径下的task.json文件
3. 如需GPU监控，确保nvidia-smi命令可用
4. Python环境（2.7或3.x）
5. 确保程序运行目录有写入权限（用于创建module_resource目录）

### 部署建议
- 建议使用systemd或其他进程管理工具管理程序运行
- 定期清理旧的日志文件，避免磁盘空间占用过多
- 可根据需要调整监控间隔，平衡数据精度和系统负载
- 建议设置日志轮转，避免日志文件过大

## 验证结果

程序已在生产环境验证，成功监控容器`wemol_rc_task_gpu_132178_182060_334177`：
- ✅ 正确识别GPU：NVIDIA GeForce RTX 3090
- ✅ 准确记录GPU内存使用：522MB / 24576MB
- ✅ 实时监控GPU利用率：88-89%
- ✅ 完整获取GPU状态：温度72°C、风扇54%、功耗341W
- ✅ CSV文件按模块分类存储到`module_resource/MD (GMX2024)/`
- ✅ 包含完整的22个字段数据
- ✅ 模块名称成功读取："MD (GMX2024)"
- ✅ 时间间隔控制准确，实际间隔接近设定值

## 时间间隔说明

**实际记录间隔 ≈ 设定间隔**

程序已优化时间控制：
- 记录每轮开始时间
- 计算程序执行时间（通常1-2秒）
- 动态调整等待时间
- 确保总间隔接近设定值

例如：设定5秒间隔
- 程序执行：2秒
- 动态等待：3秒
- 实际间隔：≈5秒

## 注意事项

1. 程序需要持续运行以进行定期监控
2. 使用Ctrl+C可以优雅地停止程序
3. 首次运行会自动创建`module_resource`目录结构
4. 如果容器名称格式不符合预期，请检查日志文件获取详细错误信息
5. GPU监控需要容器进程实际使用GPU，空闲时可能显示N/A
6. 文件夹名称会自动清理特殊字符，确保跨平台兼容性

## 示例输出

### 控制台输出
```
2024-01-15 10:30:00,123 - INFO - 开始wemol资源监控...
2024-01-15 10:30:00,456 - INFO - 找到 1 个wemol_rc_task容器
2024-01-15 10:30:00,789 - INFO - 找到任务文件: /data/PRG/RCall/Worker.GPU/work_blob/21/78/132178/task.json
2024-01-15 10:30:01,012 - INFO - 任务 132178 的模块名称: MD (GMX2024)
2024-01-15 10:30:01,234 - INFO - 创建CSV文件: module_resource/MD (GMX2024)/132178.csv (模块: MD (GMX2024))
2024-01-15 10:30:01,567 - DEBUG - 本轮执行时间: 2.15s, 等待时间: 2.85s
```

### 目录结构示例
```
/data/PRG/tools/wemol_resource_watcher/
├── module_resource/
│   ├── MD (GMX2024)/
│   │   ├── 132178.csv
│   │   └── 145632.csv
│   ├── Protein Folding/
│   │   └── 156789.csv
│   └── Unknown_Module/
│       └── 167890.csv
├── wemol_resource_monitor.log
└── recorder.py
```

### CSV文件内容示例
```csv
task_id,job_id,module_name,timestamp,container,cpu_percent,mem_usage,mem_percent,net_io,block_io,pids,gpu_count,gpu_ids,gpu_names,gpu_memory_used,gpu_memory_total,gpu_utilization,gpu_memory_utilization,gpu_temperature,gpu_fan_speed,gpu_power_draw,gpu_power_limit
132178,182060,MD (GMX2024),2025-06-04 09:44:28,wemol_rc_task_gpu_132178_182060_334177,3189.83%,975.7MiB / 250.3GiB,0.38%,746B / 0B,1.58MB / 1.71GB,39,1,0,NVIDIA GeForce RTX 3090,522MB,24576MB,88%,35%,72°C,54%,341.52W,350.00W
```

## 更新历史

- v1.0: 初始版本，基本容器监控功能
- v1.1: 修复Python兼容性问题
- v1.2: 优化Docker命令格式，解决输出解析问题
- v1.3: 增强GPU监控功能，解决初始N/A问题
- v1.4: 添加GPU利用率实时监控，完善错误处理机制
- v2.0: **重大更新**
  - 扩展GPU监控，获取nvidia-smi全部信息（温度、风扇、功耗等）
  - 实现按模块名分类存储CSV文件
  - 优化时间间隔控制，确保监控精度
  - 增加文件名清理和目录管理功能
  - 完善日志输出和错误处理 