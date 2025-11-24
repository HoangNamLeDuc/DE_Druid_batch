# Chọn Python 3.12 (hoặc bản bạn muốn)
FROM python:3.12-slim

# Tạo thư mục chứa code trong container
WORKDIR /app

# Copy file requirement trước để build cache
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY . .

# Thư mục data nằm trong container ở /app/data
# Main script của bạn
CMD ["python", "main.py"]
