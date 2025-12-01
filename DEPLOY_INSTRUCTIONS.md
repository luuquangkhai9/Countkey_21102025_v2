# Hướng dẫn Triển khai Countkey lên Kubernetes

Tài liệu này hướng dẫn cách xây dựng Docker image và triển khai ứng dụng Countkey lên Kubernetes sử dụng Helm Chart.

## 1. Yêu cầu chuẩn bị

- **Docker**: Đã cài đặt và đăng nhập vào Registry của công ty (nếu có).
- **Kubernetes Cluster**: Đã có quyền truy cập (file `~/.kube/config` đã được cấu hình).
- **Helm**: Đã cài đặt Helm client.
- **Kubectl**: Đã cài đặt Kubectl.

## 2. Xây dựng và Push Docker Image

Trước khi triển khai, bạn cần đóng gói ứng dụng thành Docker image và đẩy lên Registry để Kubernetes có thể tải về.

1.  **Đăng nhập vào Docker Registry** (Thay thế bằng thông tin registry của công ty bạn):
    ```bash
    docker login <your-registry-url>
    ```

2.  **Build Image**:
    ```bash
    # Ví dụ: build với tag là ngày tháng hoặc version
    docker build -t <your-registry-url>/countkey:v1.0.0 .
    ```

3.  **Push Image**:
    ```bash
    docker push <your-registry-url>/countkey:v1.0.0
    ```

## 3. Cấu hình Helm Chart

File cấu hình chính nằm tại `helm/countkey/values.yaml`. Bạn cần cập nhật các thông tin sau:

1.  **Image**: Cập nhật `repository` và `tag` khớp với image bạn vừa push.
    ```yaml
    image:
      repository: <your-registry-url>/countkey
      tag: "v1.0.0"
    ```

2.  **Cấu hình Môi trường (Config & Secrets)**:
    Kiểm tra phần `config` và `secrets` trong `values.yaml`. Đảm bảo các giá trị như `ELASTICSEARCH_URL`, `REDIS_HOST` chính xác với môi trường production của công ty.

3.  **Ingress (Domain)**:
    Nếu bạn muốn truy cập ứng dụng qua domain, hãy cấu hình phần `ingress`:
    ```yaml
    ingress:
      enabled: true
      hosts:
        - host: countkey.company.com
          paths:
            - path: /
              pathType: Prefix
    ```

## 4. Triển khai lên Kubernetes (Thủ công)

Bạn có thể triển khai bằng lệnh Helm. Có 2 cách để cập nhật phiên bản Image:

**Cách 1: Sửa file `values.yaml` (Khuyên dùng cho người mới)**
1. Mở file `helm/countkey/values.yaml`.
2. Sửa phần `tag: "v1.0.0"` thành tag mới bạn vừa push.
3. Chạy lệnh:
   ```bash
   helm upgrade --install countkey ./helm/countkey --namespace <your-namespace> --create-namespace
   ```

**Cách 2: Dùng tham số dòng lệnh (Nhanh gọn, giống script)**
Không cần sửa file `values.yaml`, bạn truyền trực tiếp tag vào lệnh deploy:

```bash
helm upgrade --install countkey ./helm/countkey \
  --namespace <your-namespace> \
  --create-namespace \
  --set image.repository=<your-registry-url>/countkey \
  --set image.tag=v1.0.0
```

**Gỡ bỏ:**
```bash
helm uninstall countkey --namespace <your-namespace>
```

## 5. Kiểm tra trạng thái

Sau khi deploy, kiểm tra xem các Pods đã chạy chưa:

```bash
kubectl get pods -n <your-namespace>
kubectl get svc -n <your-namespace>
kubectl get ingress -n <your-namespace>
```

## 6. Script tự động hóa

Bạn có thể sử dụng script `deploy.sh` đi kèm để thực hiện nhanh các bước trên.

```bash
chmod +x deploy.sh
./deploy.sh
```
(Lưu ý: Cần chỉnh sửa các biến trong script `deploy.sh` trước khi chạy)

## 7. Xem Logs và Debug

Để theo dõi log của ứng dụng khi đang chạy trên Kubernetes, bạn sử dụng lệnh `kubectl logs`.

**Bước 1: Lấy tên Pod**
```bash
kubectl get pods -n <your-namespace>
```
*Copy tên pod (ví dụ: `countkey-5d4f8c7b9-abcde`)*

**Bước 2: Xem log**

*   **Xem toàn bộ log hiện tại:**
    ```bash
    kubectl logs <pod-name> -n <your-namespace>
    ```

*   **Xem log và theo dõi liên tục (giống `tail -f`):**
    ```bash
    kubectl logs -f <pod-name> -n <your-namespace>
    ```

*   **Xem log của tất cả các pod thuộc ứng dụng (rất tiện khi có nhiều replicas):**
    ```bash
    kubectl logs -f -l app.kubernetes.io/name=countkey -n <your-namespace>
    ```

*   **Xem 100 dòng log cuối cùng:**
    ```bash
    kubectl logs --tail=100 <pod-name> -n <your-namespace>
    ```

*   **Nếu Pod bị crash và restart, xem log của lần chạy trước:**
    ```bash
    kubectl logs <pod-name> --previous -n <your-namespace>
    ```
