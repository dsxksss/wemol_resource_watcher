#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
wemol平台作业模块调度资源监控记录程序

该程序用于监控wemol_rc_task前缀的Docker容器资源使用情况，
并将监控数据记录到CSV文件中，同时从task.json文件中读取作业信息。
"""

import subprocess
import json
import csv
import time
import re
import os
import logging
from typing import Dict, List, Optional
from datetime import datetime


class WemolResourceRecorder:
    """Wemol资源监控记录器"""

    def __init__(self, interval: int = 5, log_level: str = "INFO"):
        """
        初始化监控记录器

        Args:
            interval: 监控间隔时间（秒），默认5秒
            log_level: 日志级别，默认INFO
        """
        self.interval = interval
        self.setup_logging(log_level)
        self.csv_files: Dict[str, str] = {}  # task_id -> csv_file_path
        self.module_names: Dict[str, str] = {}  # task_id -> module_name

    def setup_logging(self, level: str):
        """设置日志配置"""
        logging.basicConfig(
            level=getattr(logging, level.upper()),
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("wemol_resource_monitor.log"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def get_wemol_containers(self) -> List[Dict[str, str]]:
        """
        获取所有wemol_rc_task前缀的容器信息

        Returns:
            包含容器信息的字典列表，每个字典包含container_id, name, task_id, job_id
        """
        try:
            # 执行docker ps命令获取容器信息，使用简单格式避免表格对齐问题
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.ID}} {{.Names}}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            self.logger.debug(f"Docker ps 原始输出:\n{result.stdout}")

            containers = []
            lines = result.stdout.strip().split("\n")

            self.logger.debug(f"处理的行数: {len(lines)}")

            for i, line in enumerate(lines):
                self.logger.debug(f"处理第{i+1}行: '{line}'")

                if not line.strip():
                    self.logger.debug(f"跳过空行")
                    continue

                # 使用空格分割，限制为2部分（容器ID和容器名）
                parts = line.strip().split(" ", 1)
                self.logger.debug(f"分割后的部分: {parts}")

                if len(parts) >= 2:
                    container_id = parts[0]
                    container_name = parts[1]
                    self.logger.debug(
                        f"容器ID: '{container_id}', 容器名: '{container_name}'"
                    )

                    # 检查是否是wemol_rc_task前缀的容器
                    if container_name.startswith("wemol_rc_task"):
                        self.logger.debug(f"找到wemol_rc_task容器: {container_name}")

                        # 解析容器名称中的数字: wemol_rc_task_gpu_132178_182060_334177
                        # 如果容器名称是: wemol_rc_task_132178_182060_334177
                        # 则后续判断都按 wemol_rc_task_all 处理
                        # 只关注前两个数字：task_id 和 job_id
                        pattern = r"wemol_rc_task_\w+_(\d+)_(\d+)_\d+"
                        match = re.match(pattern, container_name)

                        self.logger.debug(f"正则匹配结果: {match}")

                        if match:
                            task_id = match.group(1)  # 第一个数字作为task_id
                            job_id = match.group(2)  # 第二个数字作为job_id

                            self.logger.debug(
                                f"解析得到 task_id: {task_id}, job_id: {job_id}"
                            )

                            containers.append(
                                {
                                    "container_id": container_id,
                                    "name": container_name,
                                    "task_id": task_id,
                                    "job_id": job_id,
                                }
                            )
                        elif container_name.startswith("wemol_rc_task"):
                            task_id = container_name.split("_")[3]
                            job_id = container_name.split("_")[4]

                            self.logger.debug(
                                f"解析得到 task_id: {task_id}, job_id: {job_id}"
                            )

                            containers.append(
                                {
                                    "container_id": container_id,
                                    "name": container_name,
                                    "task_id": task_id,
                                    "job_id": job_id,
                                }
                            )
                        else:
                            self.logger.warning(
                                f"容器名称 {container_name} 不匹配预期格式"
                            )
                    else:
                        self.logger.debug(f"跳过非wemol_rc_task容器: {container_name}")
                else:
                    self.logger.debug(f"行分割后部分数量不足: {len(parts)}")

            self.logger.info(f"找到 {len(containers)} 个wemol_rc_task容器")
            return containers

        except subprocess.CalledProcessError as e:
            self.logger.error(f"执行docker ps命令失败: {e}")
            return []
        except Exception as e:
            self.logger.error(f"获取容器信息时出错: {e}")
            return []

    def get_task_info(self, task_id: str) -> Optional[Dict]:
        """
        从task.json文件中读取任务信息

        Args:
            task_id: 任务ID（例如132178）

        Returns:
            任务信息字典，如果读取失败返回None
        """
        try:
            # 构建文件路径
            # 路径规则: /data/PRG/RCall/Worker.{类型}/work_blob/{倒数四位的前两位}/{倒数两位}/{完整数字}/task.json
            if len(task_id) < 4:
                self.logger.warning(f"任务ID {task_id} 长度不足4位，无法构建路径")
                return None

            # 提取路径组件
            last_two = task_id[-2:]  # 倒数两位
            second_last_two = task_id[-4:-2]  # 倒数四位的前两位

            # 尝试不同的Worker类型
            worker_types = ["GPU", "CPU", "AF2", "ALL"]

            for worker_type in worker_types:
                task_file_path = f"/data/PRG/RCall/Worker.{worker_type}/work_blob/{second_last_two}/{last_two}/{task_id}/task.json"

                if os.path.exists(task_file_path):
                    self.logger.info(f"找到任务文件: {task_file_path}")

                    with open(task_file_path, "r", encoding="utf-8") as f:
                        task_data = json.load(f)
                        return task_data

            self.logger.warning(f"未找到任务ID {task_id} 对应的task.json文件")
            return None

        except json.JSONDecodeError as e:
            self.logger.error(f"解析task.json文件失败: {e}")
            return None
        except Exception as e:
            self.logger.error(f"读取任务信息时出错: {e}")
            return None

    def get_module_name(self, task_id: str) -> str:
        """
        获取模块名称，如果已缓存则直接返回，否则从task.json读取

        Args:
            task_id: 任务ID

        Returns:
            模块名称，如果获取失败返回"Unknown"
        """
        if task_id in self.module_names:
            return self.module_names[task_id]

        task_info = self.get_task_info(task_id)
        if task_info and "Module" in task_info and "Name" in task_info["Module"]:
            module_name = task_info["Module"]["Name"]
            self.module_names[task_id] = module_name
            self.logger.info(f"任务 {task_id} 的模块名称: {module_name}")
            return module_name
        else:
            self.logger.warning(f"无法获取任务 {task_id} 的模块名称")
            return "Unknown"

    def get_container_stats(self, container_name: str) -> Optional[Dict]:
        """
        获取容器的资源使用统计信息

        Args:
            container_name: 容器名称

        Returns:
            包含资源统计信息的字典，如果获取失败返回None
        """
        try:
            # 使用docker stats --no-stream --format获取一次性统计信息，使用简单格式
            result = subprocess.run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.Container}} {{.CPUPerc}} {{.MemUsage}} {{.MemPerc}} {{.NetIO}} {{.BlockIO}} {{.PIDs}}",
                    container_name,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            self.logger.debug(f"Docker stats 原始输出:\n{result.stdout}")
            self.logger.debug(f"Docker stats 错误输出:\n{result.stderr}")

            lines = result.stdout.strip().split("\n")
            self.logger.debug(f"Docker stats 行数: {len(lines)}")

            for i, line in enumerate(lines):
                self.logger.debug(f"Stats第{i+1}行: '{line}'")

            if len(lines) >= 1:  # 只有数据行，没有表头
                data_line = lines[0]
                # 使用空格分割，但需要处理内存使用量中的空格（如 "951.7MiB / 250.3GiB"）
                # 先按空格分割，然后合并内存相关的部分
                parts = data_line.split()
                self.logger.debug(f"数据行分割后: {parts}")

                if len(parts) >= 7:
                    # 重新组合，因为内存使用量被分割了
                    # 格式: container cpu_percent mem1 / mem2 mem_percent net1 / net2 block1 / block2 pids
                    container = parts[0]
                    cpu_percent = parts[1]
                    # 内存使用量通常是 "数字单位 / 数字单位" 的格式，可能被分割为3部分
                    mem_usage = (
                        f"{parts[2]} {parts[3]} {parts[4]}"  # "951.7MiB / 250.3GiB"
                    )
                    mem_percent = parts[5]
                    # 网络IO也是 "数字 / 数字" 格式
                    net_io = f"{parts[6]} {parts[7]} {parts[8]}"  # "746B / 0B"
                    # 块IO也是 "数字 / 数字" 格式
                    block_io = f"{parts[9]} {parts[10]} {parts[11]}"  # "848kB / 254MB"
                    pids = parts[12]

                    stats_dict = {
                        "container": container,
                        "cpu_percent": cpu_percent,
                        "mem_usage": mem_usage,
                        "mem_percent": mem_percent,
                        "net_io": net_io,
                        "block_io": block_io,
                        "pids": pids,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    self.logger.debug(f"解析得到的统计信息: {stats_dict}")
                    return stats_dict
                else:
                    self.logger.warning(f"数据行分割后部分数量不足: {len(parts)}")
            else:
                self.logger.warning(f"Docker stats输出行数不足: {len(lines)}")

            return None

        except subprocess.CalledProcessError as e:
            self.logger.error(f"获取容器 {container_name} 统计信息失败: {e}")
            self.logger.error(
                f"命令执行错误输出: {e.stderr if hasattr(e, 'stderr') else '无'}"
            )
            return None
        except Exception as e:
            self.logger.error(f"处理容器统计信息时出错: {e}")
            return None

    def sanitize_folder_name(self, name: str) -> str:
        """
        清理模块名称，使其适合作为文件夹名称

        Args:
            name: 原始名称

        Returns:
            清理后的名称
        """
        import re

        # 移除或替换不适合做文件夹名的字符
        # 保留字母、数字、中文字符、空格、连字符、下划线和圆括号
        sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)
        # 替换多个连续空格为单个空格
        sanitized = re.sub(r"\s+", " ", sanitized)
        # 去除首尾空格
        sanitized = sanitized.strip()
        # 如果为空，使用默认名称
        if not sanitized:
            sanitized = "Unknown_Module"
        return sanitized

    def setup_csv_file(self, task_id: str) -> str:
        """
        设置CSV文件，按模块名分类存储

        Args:
            task_id: 任务ID

        Returns:
            CSV文件路径
        """
        # 获取模块名称
        module_name = self.get_module_name(task_id)

        # 清理模块名称，使其适合作为文件夹名
        safe_module_name = self.sanitize_folder_name(module_name)

        # 创建目录结构: module_resource/{module_name}/
        module_dir = os.path.join("module_resource", safe_module_name)
        os.makedirs(module_dir, exist_ok=True)

        # CSV文件路径: module_resource/{module_name}/{task_id}.csv
        csv_filename = os.path.join(module_dir, f"{task_id}.csv")

        # 如果文件不存在，创建并写入表头
        if not os.path.exists(csv_filename):
            with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = [
                    "task_id",
                    "job_id",
                    "module_name",
                    "timestamp",
                    "container",
                    "cpu_percent",
                    "mem_usage",
                    "mem_percent",
                    "net_io",
                    "block_io",
                    "pids",
                    "gpu_count",
                    "gpu_ids",
                    "gpu_names",
                    "gpu_memory_used",
                    "gpu_memory_total",
                    "gpu_utilization",
                    "gpu_memory_utilization",
                    "gpu_temperature",
                    "gpu_fan_speed",
                    "gpu_power_draw",
                    "gpu_power_limit",
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

            self.logger.info(f"创建CSV文件: {csv_filename} (模块: {module_name})")

        return csv_filename

    def record_stats(self, container_info: Dict, stats: Dict):
        """
        将统计信息记录到CSV文件

        Args:
            container_info: 容器信息
            stats: 统计信息
        """
        try:
            task_id = container_info["task_id"]
            csv_file = self.setup_csv_file(task_id)

            # 获取模块名称
            module_name = self.get_module_name(task_id)

            # 获取GPU信息
            gpu_info = self.get_gpu_info_for_container(container_info["name"])

            # 准备记录数据
            record = {
                "task_id": task_id,
                "job_id": container_info["job_id"],
                "module_name": module_name,
                "timestamp": stats["timestamp"],
                "container": stats["container"],
                "cpu_percent": stats["cpu_percent"],
                "mem_usage": stats["mem_usage"],
                "mem_percent": stats["mem_percent"],
                "net_io": stats["net_io"],
                "block_io": stats["block_io"],
                "pids": stats["pids"],
                "gpu_count": gpu_info["gpu_count"],
                "gpu_ids": gpu_info["gpu_ids"],
                "gpu_names": gpu_info["gpu_names"],
                "gpu_memory_used": gpu_info["gpu_memory_used"],
                "gpu_memory_total": gpu_info["gpu_memory_total"],
                "gpu_utilization": gpu_info["gpu_utilization"],
                "gpu_memory_utilization": gpu_info["gpu_memory_utilization"],
                "gpu_temperature": gpu_info["gpu_temperature"],
                "gpu_fan_speed": gpu_info["gpu_fan_speed"],
                "gpu_power_draw": gpu_info["gpu_power_draw"],
                "gpu_power_limit": gpu_info["gpu_power_limit"],
            }

            # 写入CSV文件
            with open(csv_file, "a", newline="", encoding="utf-8") as csvfile:
                fieldnames = list(record.keys())
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(record)

            self.logger.debug(f"记录数据到 {csv_file}: {record}")

        except Exception as e:
            self.logger.error(f"记录统计信息时出错: {e}")

    def get_container_processes(self, container_name: str) -> List[str]:
        """
        获取容器内部运行的进程PID列表

        Args:
            container_name: 容器名称

        Returns:
            进程PID列表
        """
        try:
            # 方法1: 使用docker top获取容器内进程
            result = subprocess.run(
                ["docker", "top", container_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            self.logger.debug(f"Docker top {container_name} 输出:\n{result.stdout}")

            pids = []
            lines = result.stdout.strip().split("\n")

            # 跳过表头
            if len(lines) > 1:
                for line in lines[1:]:
                    parts = line.split()
                    if len(parts) >= 2:
                        # docker top的输出格式通常是：UID PID PPID C STIME TTY TIME CMD
                        pid = parts[1]  # 第二列是PID
                        if pid.isdigit():
                            pids.append(pid)
                            self.logger.debug(
                                f"找到容器 {container_name} 的进程PID: {pid}"
                            )

            # 方法2: 如果docker top失败，尝试通过ps aux查找
            if not pids:
                self.logger.debug(f"Docker top未找到进程，尝试ps aux方法")
                result2 = subprocess.run(
                    ["ps", "aux"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    check=True,
                )

                lines = result2.stdout.strip().split("\n")
                for line in lines:
                    # 查找包含容器名称或容器ID的进程
                    if container_name in line or container_name[:12] in line:
                        parts = line.split()
                        if len(parts) >= 2:
                            pid = parts[1]
                            if pid.isdigit():
                                pids.append(pid)
                                self.logger.debug(
                                    f"通过ps aux找到容器 {container_name} 的进程PID: {pid}"
                                )

            self.logger.debug(
                f"容器 {container_name} 总共找到 {len(pids)} 个进程: {pids}"
            )
            return pids

        except subprocess.CalledProcessError as e:
            self.logger.error(f"获取容器 {container_name} 进程信息失败: {e}")
            return []
        except Exception as e:
            self.logger.error(f"处理容器进程信息时出错: {e}")
            return []

    def get_nvidia_smi_info(self) -> Dict[str, Dict]:
        """
        获取nvidia-smi的GPU使用信息

        Returns:
            PID到GPU信息的映射字典
        """
        try:
            # 方法1: 使用nvidia-smi查询进程信息
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-compute-apps=pid,gpu_uuid,used_memory",
                    "--format=csv,noheader,nounits",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            self.logger.debug(f"nvidia-smi query-compute-apps 输出:\n{result.stdout}")

            pid_gpu_map = {}
            lines = result.stdout.strip().split("\n")

            for line in lines:
                if line.strip():
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 3:
                        try:
                            pid = parts[0]
                            gpu_uuid = parts[1]
                            used_memory = parts[2]

                            if pid.isdigit():
                                # 获取GPU ID（从UUID中提取或使用简单编号）
                                gpu_id = self.get_gpu_id_from_uuid(gpu_uuid)

                                pid_gpu_map[pid] = {
                                    "gpu_id": gpu_id,
                                    "gpu_uuid": gpu_uuid,
                                    "used_memory": used_memory,
                                    "sm_util": "N/A",  # compute-apps查询不包含利用率
                                    "mem_util": "N/A",
                                }
                                self.logger.debug(
                                    f"GPU进程映射: PID={pid} GPU={gpu_id} MEM={used_memory}MB"
                                )
                        except (ValueError, IndexError):
                            continue

            # 方法2: 如果上面的方法没有结果，尝试pmon方式
            if not pid_gpu_map:
                self.logger.debug("尝试使用nvidia-smi pmon方法")
                result2 = subprocess.run(
                    ["nvidia-smi", "pmon", "-c", "1", "-s", "um"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    check=True,
                )

                self.logger.debug(f"nvidia-smi pmon 输出:\n{result2.stdout}")

                lines = result2.stdout.strip().split("\n")
                for line in lines:
                    # 跳过注释行和空行
                    if line.startswith("#") or not line.strip():
                        continue

                    parts = line.split()
                    if len(parts) >= 4:
                        try:
                            gpu_id = parts[0]
                            pid = parts[1]
                            sm_util = parts[3] if len(parts) > 3 else "0"
                            mem_util = parts[4] if len(parts) > 4 else "0"

                            if pid.isdigit() and pid != "-":
                                pid_gpu_map[pid] = {
                                    "gpu_id": gpu_id,
                                    "gpu_uuid": "N/A",
                                    "used_memory": "N/A",
                                    "sm_util": sm_util,
                                    "mem_util": mem_util,
                                }
                                self.logger.debug(
                                    f"GPU进程映射(pmon): PID={pid} GPU={gpu_id} SM={sm_util}% MEM={mem_util}%"
                                )
                        except (ValueError, IndexError):
                            continue

            return pid_gpu_map

        except subprocess.CalledProcessError as e:
            self.logger.debug(f"nvidia-smi命令执行失败: {e}")
            return {}
        except Exception as e:
            self.logger.debug(f"处理nvidia-smi信息时出错: {e}")
            return {}

    def get_gpu_id_from_uuid(self, gpu_uuid: str) -> str:
        """
        从GPU UUID获取GPU ID

        Args:
            gpu_uuid: GPU UUID

        Returns:
            GPU ID字符串
        """
        try:
            # 尝试获取GPU列表来映射UUID到ID
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=index,uuid",
                    "--format=csv,noheader,nounits",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            lines = result.stdout.strip().split("\n")
            for line in lines:
                if line.strip():
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 2:
                        gpu_id = parts[0]
                        uuid = parts[1]
                        if uuid == gpu_uuid:
                            return gpu_id

            # 如果找不到匹配的UUID，返回"Unknown"
            return "Unknown"

        except:
            # 如果出错，返回UUID的简短形式
            return gpu_uuid[-8:] if len(gpu_uuid) > 8 else gpu_uuid

    def get_gpu_detailed_info(self) -> Dict[str, Dict]:
        """
        获取详细的GPU信息，包括温度、风扇、功耗等

        Returns:
            GPU ID到详细信息的映射字典
        """
        try:
            # 查询详细的GPU信息
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=index,name,memory.total,memory.used,utilization.gpu,utilization.memory,temperature.gpu,fan.speed,power.draw,power.limit",
                    "--format=csv,noheader,nounits",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            self.logger.debug(f"nvidia-smi 详细信息输出:\n{result.stdout}")

            gpu_info_map = {}
            lines = result.stdout.strip().split("\n")

            for line in lines:
                if line.strip():
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 10:
                        try:
                            gpu_id = parts[0]
                            gpu_name = parts[1]
                            memory_total = parts[2]
                            memory_used = parts[3]
                            gpu_util = parts[4]
                            mem_util = parts[5]
                            temperature = parts[6]
                            fan_speed = parts[7]
                            power_draw = parts[8]
                            power_limit = parts[9]

                            gpu_info_map[gpu_id] = {
                                "name": gpu_name,
                                "memory_total": memory_total,
                                "memory_used": memory_used,
                                "gpu_util": gpu_util,
                                "mem_util": mem_util,
                                "temperature": temperature,
                                "fan_speed": fan_speed,
                                "power_draw": power_draw,
                                "power_limit": power_limit,
                            }

                            self.logger.debug(
                                f"GPU {gpu_id} 详细信息: {gpu_info_map[gpu_id]}"
                            )
                        except (ValueError, IndexError) as e:
                            self.logger.debug(f"解析GPU信息行失败: {line}, 错误: {e}")
                            continue

            return gpu_info_map

        except subprocess.CalledProcessError as e:
            self.logger.debug(f"获取GPU详细信息失败: {e}")
            return {}
        except Exception as e:
            self.logger.debug(f"处理GPU详细信息时出错: {e}")
            return {}

    def get_gpu_info_for_container(self, container_name: str) -> Dict[str, str]:
        """
        获取容器的GPU使用信息

        Args:
            container_name: 容器名称

        Returns:
            GPU使用信息字典
        """
        # 获取容器进程PID
        container_pids = self.get_container_processes(container_name)

        # 获取GPU进程信息
        gpu_processes = self.get_nvidia_smi_info()

        # 获取详细的GPU信息
        gpu_detailed_info = self.get_gpu_detailed_info()

        # 匹配PID并收集GPU信息
        gpu_ids = []
        gpu_names = []
        gpu_memory_used = []
        gpu_memory_total = []
        gpu_utilizations = []
        gpu_memory_utilizations = []
        gpu_temperatures = []
        gpu_fan_speeds = []
        gpu_power_draws = []
        gpu_power_limits = []

        for pid in container_pids:
            if pid in gpu_processes:
                gpu_info = gpu_processes[pid]
                gpu_id = gpu_info["gpu_id"]

                # 避免重复添加同一个GPU
                if gpu_id not in gpu_ids:
                    gpu_ids.append(gpu_id)

                    # 从详细信息中获取数据
                    if gpu_id in gpu_detailed_info:
                        detailed = gpu_detailed_info[gpu_id]
                        gpu_names.append(detailed["name"])
                        gpu_memory_used.append(f"{detailed['memory_used']}MB")
                        gpu_memory_total.append(f"{detailed['memory_total']}MB")
                        gpu_utilizations.append(f"{detailed['gpu_util']}%")
                        gpu_memory_utilizations.append(f"{detailed['mem_util']}%")
                        gpu_temperatures.append(f"{detailed['temperature']}°C")

                        # 风扇转速可能为"N/A"或"[Not Supported]"
                        fan_speed = detailed["fan_speed"]
                        if fan_speed and fan_speed not in ["N/A", "[Not Supported]"]:
                            gpu_fan_speeds.append(f"{fan_speed}%")
                        else:
                            gpu_fan_speeds.append("N/A")

                        gpu_power_draws.append(f"{detailed['power_draw']}W")
                        gpu_power_limits.append(f"{detailed['power_limit']}W")
                    else:
                        # 如果没有详细信息，使用N/A填充
                        gpu_names.append("Unknown")
                        gpu_memory_used.append("N/A")
                        gpu_memory_total.append("N/A")
                        gpu_utilizations.append("N/A")
                        gpu_memory_utilizations.append("N/A")
                        gpu_temperatures.append("N/A")
                        gpu_fan_speeds.append("N/A")
                        gpu_power_draws.append("N/A")
                        gpu_power_limits.append("N/A")

                    self.logger.debug(
                        f"容器 {container_name} 使用GPU {gpu_id}: {gpu_info}"
                    )

        # 汇总GPU使用情况
        result = {
            "gpu_count": str(len(gpu_ids)),
            "gpu_ids": ",".join(gpu_ids) if gpu_ids else "N/A",
            "gpu_names": ",".join(gpu_names) if gpu_names else "N/A",
            "gpu_memory_used": ",".join(gpu_memory_used) if gpu_memory_used else "N/A",
            "gpu_memory_total": (
                ",".join(gpu_memory_total) if gpu_memory_total else "N/A"
            ),
            "gpu_utilization": (
                ",".join(gpu_utilizations) if gpu_utilizations else "N/A"
            ),
            "gpu_memory_utilization": (
                ",".join(gpu_memory_utilizations) if gpu_memory_utilizations else "N/A"
            ),
            "gpu_temperature": (
                ",".join(gpu_temperatures) if gpu_temperatures else "N/A"
            ),
            "gpu_fan_speed": ",".join(gpu_fan_speeds) if gpu_fan_speeds else "N/A",
            "gpu_power_draw": ",".join(gpu_power_draws) if gpu_power_draws else "N/A",
            "gpu_power_limit": (
                ",".join(gpu_power_limits) if gpu_power_limits else "N/A"
            ),
        }

        self.logger.debug(f"容器 {container_name} GPU汇总信息: {result}")
        return result

    def get_gpu_utilization(self) -> Dict[str, Dict]:
        """
        获取GPU利用率信息（保留用于兼容性）

        Returns:
            GPU ID到利用率信息的映射字典
        """
        try:
            # 使用nvidia-smi查询GPU利用率
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=index,utilization.gpu,utilization.memory",
                    "--format=csv,noheader,nounits",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            self.logger.debug(f"nvidia-smi GPU利用率输出:\n{result.stdout}")

            gpu_util_map = {}
            lines = result.stdout.strip().split("\n")

            for line in lines:
                if line.strip():
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 3:
                        try:
                            gpu_id = parts[0]
                            gpu_util = parts[1]
                            mem_util = parts[2]

                            gpu_util_map[gpu_id] = {
                                "gpu_util": gpu_util,
                                "mem_util": mem_util,
                            }
                            self.logger.debug(
                                f"GPU {gpu_id} 利用率: GPU={gpu_util}% MEM={mem_util}%"
                            )
                        except (ValueError, IndexError):
                            continue

            return gpu_util_map

        except subprocess.CalledProcessError as e:
            self.logger.debug(f"获取GPU利用率失败: {e}")
            return {}
        except Exception as e:
            self.logger.debug(f"处理GPU利用率时出错: {e}")
            return {}

    def run_monitoring(self):
        """
        运行监控循环
        """
        self.logger.info("开始wemol资源监控...")

        try:
            while True:
                # 记录开始时间
                start_time = time.time()

                # 获取所有wemol容器
                containers = self.get_wemol_containers()

                if not containers:
                    self.logger.warning("未找到任何wemol_rc_task容器，等待下次检查...")
                else:
                    # 为每个容器获取统计信息并记录
                    for container in containers:
                        stats = self.get_container_stats(container["name"])
                        if stats:
                            self.record_stats(container, stats)
                        else:
                            self.logger.warning(
                                f"无法获取容器 {container['name']} 的统计信息"
                            )

                # 计算已执行时间
                execution_time = time.time() - start_time

                # 计算需要sleep的时间，确保总间隔为设定值
                sleep_time = max(0, self.interval - execution_time)

                if execution_time > self.interval:
                    self.logger.warning(
                        f"程序执行时间({execution_time:.2f}s)超过设定间隔({self.interval}s)"
                    )
                else:
                    self.logger.debug(
                        f"本轮执行时间: {execution_time:.2f}s, 等待时间: {sleep_time:.2f}s"
                    )

                # 等待下次监控
                time.sleep(sleep_time)

        except KeyboardInterrupt:
            self.logger.info("收到中断信号，停止监控...")
        except Exception as e:
            self.logger.error(f"监控过程中出现错误: {e}")
            raise


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Wemol平台作业模块调度资源监控记录程序"
    )
    parser.add_argument(
        "--interval", type=int, default=5, help="监控间隔时间（秒），默认5秒"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="日志级别，默认INFO",
    )

    args = parser.parse_args()

    # 创建监控器并开始运行
    recorder = WemolResourceRecorder(interval=args.interval, log_level=args.log_level)
    recorder.run_monitoring()


if __name__ == "__main__":
    main()
