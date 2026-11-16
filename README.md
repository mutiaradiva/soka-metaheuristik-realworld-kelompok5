# Implementasi SOKA Metaheuristic di Real World

Proyek ini menambahkan eksekusi task melalui AWS/HTTP dengan autentikasi ke scheduler CSA dan PHS berbasis Java yang sudah ada. Source CloudSim dan dataset tetap disertakan agar simulasi dan eksekusi nyata berjalan sebagai mode yang terpisah. Lihat [panduan deployment dan penggunaan](docs/real-world-execution.md).

## Memulai

1. Gunakan JDK 21 dan jalankan build dari folder `java`: `gradlew.bat build` (Windows) atau `./gradlew build` (Linux).
2. Atur `SOKA_WORKER_TOKEN` pada controller dan worker.
3. Salin `deploy/aws-workers.example.json` menjadi `deploy/aws-workers.json`, lalu isi endpoint yang dapat dijangkau dan identitas worker yang sesuai.
4. Jalankan worker service di setiap mesin, lalu jalankan `./gradlew run --args="preflight --config deploy/aws-workers.json"`.
5. Jalankan salah satu algoritma dengan perintah `./gradlew run --args="run --algorithm CSA --config deploy/aws-workers.json --dataset ../dataset/GoCJ_Dataset_1000.csv --output ../results/csa-run-01"` (ganti `CSA` dengan `PHS` untuk menjalankan PHS).

Credentials AWS dan alamat IP tidak disimpan di source code. File cluster contoh menggunakan placeholder.
