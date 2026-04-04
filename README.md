1. Clone repo
2. Copy file .env.example thành file .env, đổi tên của mình
3. Bỏ file key GCP vào config/gcs/credentials/
4. Tạo thư mục packages chứa các file nén hadoop, spark, ...
5. Chạy:
   docker compose up -d --build
