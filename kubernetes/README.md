# Hướng dẫn triển khai Kubernetes (Full)

Bộ cấu hình này bao gồm đầy đủ các thành phần: Deployment, Service, ConfigMap, Secret, Ingress, và HPA.

## 1. Cấu trúc file
- `configmap.yaml`: Chứa cấu hình không nhạy cảm (Host, Port...).
- `secret.yaml`: Chứa thông tin nhạy cảm (URL DB, Password...).
- `deployment.yaml`: Định nghĩa ứng dụng, bao gồm Resource Limits/Requests.
- `service.yaml`: Định nghĩa Service để các Pod giao tiếp.
- `ingress.yaml`: Cấu hình Ingress để truy cập từ bên ngoài (Domain).
- `hpa.yaml`: Tự động scale số lượng Pod dựa trên CPU/RAM.

## 2. Triển khai

Thực hiện theo thứ tự sau:

### Bước 1: ConfigMap & Secret
```bash
kubectl apply -f kubernetes/configmap.yaml
kubectl apply -f kubernetes/secret.yaml
```

### Bước 2: Deployment & Service
```bash
kubectl apply -f kubernetes/deployment.yaml
kubectl apply -f kubernetes/service.yaml
```

### Bước 3: Ingress (Truy cập ngoài)
Sửa file `ingress.yaml` để cập nhật domain của bạn, sau đó chạy:
```bash
kubectl apply -f kubernetes/ingress.yaml
```

### Bước 4: HPA (Auto Scaling)
```bash
kubectl apply -f kubernetes/hpa.yaml
```

## 3. Kiểm tra
```bash
kubectl get all
kubectl get ingress
kubectl get hpa
```
