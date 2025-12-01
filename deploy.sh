#!/bin/bash

# Cấu hình các biến (VUI LÒNG CHỈNH SỬA TRƯỚC KHI CHẠY)
APP_NAME="countkey"
DOCKER_REGISTRY="your-registry.com" # Thay thế bằng registry của công ty bạn
IMAGE_NAME="$DOCKER_REGISTRY/$APP_NAME"
TAG=$(date +%Y%m%d-%H%M%S) # Sử dụng timestamp làm tag
NAMESPACE="default" # Namespace k8s muốn deploy

echo "--------------------------------------------------"
echo "Bắt đầu quy trình Build & Deploy cho $APP_NAME"
echo "Registry: $DOCKER_REGISTRY"
echo "Image: $IMAGE_NAME:$TAG"
echo "Namespace: $NAMESPACE"
echo "--------------------------------------------------"

# 1. Build Docker Image
echo "[1/4] Building Docker image..."
docker build -t $IMAGE_NAME:$TAG .
if [ $? -ne 0 ]; then
    echo "Lỗi: Build Docker image thất bại."
    exit 1
fi

# 2. Push Docker Image
echo "[2/4] Pushing Docker image to registry..."
# docker login $DOCKER_REGISTRY # Bỏ comment nếu cần login tự động
docker push $IMAGE_NAME:$TAG
if [ $? -ne 0 ]; then
    echo "Lỗi: Push Docker image thất bại."
    exit 1
fi

# 3. Deploy/Upgrade Helm Chart
echo "[3/4] Deploying to Kubernetes..."

# Kiểm tra xem release đã tồn tại chưa
helm status $APP_NAME -n $NAMESPACE > /dev/null 2>&1
if [ $? -eq 0 ]; then
    ACTION="upgrade"
else
    ACTION="install"
fi

echo "Thực hiện Helm $ACTION..."

# Deploy với set image.tag mới nhất
helm $ACTION $APP_NAME ./helm/countkey \
    --namespace $NAMESPACE \
    --create-namespace \
    --set image.repository=$IMAGE_NAME \
    --set image.tag=$TAG

if [ $? -ne 0 ]; then
    echo "Lỗi: Helm deploy thất bại."
    exit 1
fi

# 4. Verify
echo "[4/4] Kiểm tra trạng thái..."
kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=$APP_NAME

echo "--------------------------------------------------"
echo "Hoàn tất! Ứng dụng đã được deploy với version $TAG"
echo "--------------------------------------------------"
