#!/usr/bin/env bash
# 作业5 一键运行脚本：打包 -> 上传输入 -> 运行 -> 下载结果
set -euo pipefail

# ---------- 环境配置（可按需修改） ----------

# Hadoop 安装目录；若 hadoop 已在 PATH 中，可留空
HADOOP_HOME="${HADOOP_HOME:-/home/legend/hadoop_installs/hadoop-3.5.0}"

# Maven 安装目录；若 mvn 已在 PATH 中，可留空
MVN_HOME="${MVN_HOME:-/home/legend/apache-maven-3.9.9}"

HADOOP_BIN="${HADOOP_HOME:+$HADOOP_HOME/bin/}hadoop"
MVN_BIN="${MVN_HOME:+$MVN_HOME/bin/}mvn"

# 项目目录（脚本所在目录）
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

JAR="target/hwk5-1.0.jar"
INPUT_A="A.txt"
INPUT_B="B.txt"

# HDFS 路径
HDFS_ROOT="/user/${USER:-$(whoami)}/hwk5"
HDFS_INPUT="$HDFS_ROOT/input"
HDFS_OUT1="$HDFS_ROOT/output_task1"
HDFS_OUT2="$HDFS_ROOT/output_task2"

# ---------- 1. 编译打包 ----------
echo "==> 编译打包"
"$MVN_BIN" -q clean package -DskipTests

# ---------- 2. 准备 HDFS 输入 ----------
echo "==> 上传输入到 HDFS"
"$HADOOP_BIN" fs -mkdir -p "$HDFS_INPUT"
"$HADOOP_BIN" fs -put -f "$INPUT_A" "$INPUT_B" "$HDFS_INPUT/"

# ---------- 3. 清理旧输出 ----------
echo "==> 清理旧输出"
"$HADOOP_BIN" fs -rm -r -f "$HDFS_OUT1" "$HDFS_OUT2"

# ---------- 4. 运行作业 ----------
echo "==> 运行作业"
"$HADOOP_BIN" jar "$JAR" hwk5.StockSetAnalysis "$HDFS_INPUT" "$HDFS_OUT1" "$HDFS_OUT2"

# ---------- 5. 下载结果到本地 output/ ----------
echo "==> 下载结果到 output/"
mkdir -p output/output_task1 output/output_task2
"$HADOOP_BIN" fs -get "$HDFS_OUT1/part-r-00000" output/output_task1/part-r-00000
"$HADOOP_BIN" fs -get "$HDFS_OUT2/part-r-00000" output/output_task2/part-r-00000

echo "==> 完成"
echo "任务1结果: output/output_task1/part-r-00000"
echo "任务2结果: output/output_task2/part-r-00000"
echo "--- 任务2统计 ---"
cat output/output_task2/part-r-00000
