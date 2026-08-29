#!/bin/bash

# 综合评分功能启动脚本

echo "正在启动 tickflow-stock-panel..."
cd "$(dirname "$0")"

# 检查 dev.sh 是否存在
if [ ! -f "./dev.sh" ]; then
    echo "错误: 找不到 dev.sh 文件"
    exit 1
fi

# 启动服务
./dev.sh

echo "服务已启动"
echo "请在浏览器中刷新页面，然后测试综合评分功能"
