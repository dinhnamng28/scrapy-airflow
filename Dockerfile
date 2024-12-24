FROM python:3.8

# Đặt thư mục làm việc
WORKDIR /app

# Copy và cài đặt các thư viện từ requirements.txt
COPY requirements.txt .  
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    rm -f requirements.txt

# Đảm bảo PATH được cập nhật
ENV PATH="/usr/local/bin:$PATH"

# Cài đặt bash làm shell mặc định (nếu cần)
SHELL ["/bin/bash", "-c"]
