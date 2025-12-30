FROM python:3.10
ENV TZ=Asia/Ho_Chi_Minh

# Cài đặt Java (yêu cầu cho VnCoreNLP)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    rm -rf /var/lib/apt/lists/*

# Đặt thư mục làm việc trong container
WORKDIR /usr/app/src

# Copy requirements trước để tận dụng Docker cache
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source code
COPY . ./

# Tải VnCoreNLP models nếu chưa có
RUN python setup_vncorenlp.py

# # Chạy ứng dụng FastAPI khi container được khởi động
# CMD ["bash", "-c", "python run.py & uvicorn main1:app --host 0.0.0.0 --port 5601 --reload"]

# Sao chép script start.sh vào container và đặt quyền thực thi
# Chuyển đổi CRLF sang LF để tránh lỗi trên Linux
COPY startup.sh /usr/app/src/startup.sh
RUN sed -i 's/\r$//' /usr/app/src/startup.sh && chmod +x /usr/app/src/startup.sh

# Chạy script start.sh khi container được khởi động
CMD ["/bin/bash", "/usr/app/src/startup.sh"]
