#!/bin/bash

#
# Copyright 2025 OPPO.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# curvine-csi 多架构镜像构建脚本

set -e

# 默认配置
DEFAULT_TAG="latest"
DEFAULT_REGISTRY="curvine"
DEFAULT_PLATFORMS="linux/amd64,linux/arm64"
DEFAULT_GOPROXY="https://goproxy.cn,direct"

# 解析命令行参数
TAG=${1:-$DEFAULT_TAG}
REGISTRY=${2:-$DEFAULT_REGISTRY}
PLATFORMS=${3:-$DEFAULT_PLATFORMS}
GOPROXY=${4:-$DEFAULT_GOPROXY}
PUSH=${5:-false}

IMAGE_NAME="${REGISTRY}/curvine-csi:${TAG}"

echo "🚀 开始构建 curvine-csi 多架构镜像"
echo "📦 镜像名称: ${IMAGE_NAME}"
echo "🏗️  目标平台: ${PLATFORMS}"
echo "🌐 Go 代理: ${GOPROXY}"
echo "📤 推送镜像: ${PUSH}"
echo ""

# 检查 Docker Buildx
if ! docker buildx version > /dev/null 2>&1; then
    echo "❌ 错误: Docker Buildx 未安装或不可用"
    echo "请安装 Docker Buildx 或使用支持多架构构建的 Docker 版本"
    exit 1
fi

# 检查是否存在 buildx builder
BUILDER_NAME="curvine-multiarch"
if ! docker buildx inspect $BUILDER_NAME > /dev/null 2>&1; then
    echo "🔧 创建多架构构建器: $BUILDER_NAME"
    docker buildx create --name $BUILDER_NAME --use --platform $PLATFORMS
else
    echo "✅ 使用现有构建器: $BUILDER_NAME"
    docker buildx use $BUILDER_NAME
fi

# 确保构建器支持所需平台
echo "🔍 检查构建器支持的平台..."
docker buildx inspect --bootstrap

# 构建命令
BUILD_CMD="docker buildx build"
BUILD_CMD="$BUILD_CMD --platform $PLATFORMS"
BUILD_CMD="$BUILD_CMD --build-arg GOPROXY=$GOPROXY"
BUILD_CMD="$BUILD_CMD -t $IMAGE_NAME"
BUILD_CMD="$BUILD_CMD -f curvine-csi/Dockerfile"
BUILD_CMD="$BUILD_CMD ."

# 是否推送
if [ "$PUSH" = "true" ]; then
    BUILD_CMD="$BUILD_CMD --push"
    echo "📤 构建并推送镜像..."
else
    BUILD_CMD="$BUILD_CMD --load"
    echo "🏗️  仅构建镜像（不推送）..."
fi

echo "🔨 执行构建命令:"
echo "   $BUILD_CMD"
echo ""

# 执行构建
eval $BUILD_CMD

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ curvine-csi 多架构镜像构建成功！"
    echo "📦 镜像: $IMAGE_NAME"
    echo "🏗️  平台: $PLATFORMS"
    
    if [ "$PUSH" = "true" ]; then
        echo "📤 镜像已推送到仓库"
    else
        echo "💾 镜像已保存到本地"
        echo ""
        echo "🔍 查看镜像信息:"
        echo "   docker images | grep curvine-csi"
        echo ""
        echo "🚀 推送镜像到仓库:"
        echo "   $0 $TAG $REGISTRY $PLATFORMS $GOPROXY true"
    fi
else
    echo ""
    echo "❌ curvine-csi 多架构镜像构建失败！"
    exit 1
fi
