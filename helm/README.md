# Hướng dẫn triển khai bằng Helm Chart

Thư mục `helm/countkey` chứa Helm Chart để triển khai ứng dụng Countkey.

## 1. Cấu trúc
- `Chart.yaml`: Thông tin về chart.
- `values.yaml`: Các giá trị cấu hình mặc định (Image, Resources, Config, Secret...).
- `templates/`: Các file template Kubernetes (Deployment, Service, Ingress...).

## 2. Cài đặt Helm (nếu chưa có)
```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

## 3. Triển khai

### Kiểm tra template (Dry run)
Để xem trước các file yaml sẽ được sinh ra:
```bash
helm install countkey-release ./helm/countkey --dry-run --debug
```

### Cài đặt (Install)
```bash
helm install countkey-release ./helm/countkey
```

### Nâng cấp (Upgrade)
Sau khi sửa đổi `values.yaml` hoặc code:
```bash
helm upgrade countkey-release ./helm/countkey
```

### Gỡ cài đặt (Uninstall)
```bash
helm uninstall countkey-release
```

## 4. Tùy biến cấu hình
Bạn có thể sửa trực tiếp file `helm/countkey/values.yaml` hoặc tạo một file `my-values.yaml` riêng và ghi đè khi cài đặt:

```bash
helm install countkey-release ./helm/countkey -f my-values.yaml
```

Ví dụ nội dung `my-values.yaml`:
```yaml
image:
  tag: "v1.0.1"
resources:
  limits:
    nvidia.com/gpu: 2
```
