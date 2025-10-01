 # ETL PIPELINE USING LUIGI END-TO-END

## Backround
Perusahaan XYZ merupakan perusahaan baru yang sedang membangun pondasi sistem data untuk mendukung kegiatan bisnis dan penelitian. Saat ini, perusahaan belum memiliki infrastruktur data yang terintegrasi dan robust, sehingga setiap tim masih mengelola data secara terpisah sesuai kebutuhan masing-masing.

Dari hasil diskusi dengan stakeholder, ditemukan bahwa:

Tim Sales sudah memiliki data penjualan di dalam database PostgreSQL, namun kualitas data masih rendah karena terdapat banyak data yang hilang (missing values) dan format data yang tidak konsisten.

Tim Product menyimpan data harga produk elektronik dalam bentuk file .csv, tetapi data tersebut berantakan, mengandung nilai kosong, dan format penyajian tidak seragam.

Tim Data Scientist membutuhkan dataset teks untuk riset Natural Language Processing (NLP), tetapi saat ini belum memiliki data sama sekali sehingga perlu dilakukan proses web scraping dari website tertentu.

Selain itu, perusahaan juga belum memiliki satu basis data terpusat untuk menyimpan hasil integrasi data, maupun pipeline otomatis untuk melakukan proses ETL (Extract, Transform, Load). Kondisi ini menyulitkan perusahaan dalam melakukan analisis bisnis, pengambilan keputusan, serta mendukung penelitian data science.

Oleh karena itu, dibutuhkan sebuah ETL Data Pipeline end-to-end yang mampu mengintegrasikan berbagai sumber data (PostgreSQL, CSV, Web Scraping), melakukan pembersihan dan transformasi data, lalu menyimpannya dalam satu database terpusat. Pipeline ini juga harus dapat dijalankan secara otomatis sesuai jadwal agar data selalu up-to-date, serta terdokumentasi dengan baik agar mudah dipahami dan digunakan oleh tim lain di perusahaan.


 ## PROBLEM
 ### > Data Sales (Database-PostgreSQL - Tim Sales)
- Data sudah ada, tapi masih banyak missing values (null/incomplete).
- Format data tidak konsisten (contoh: tanggal tidak seragam, tipe data salah, duplikasi transaksi, dsb).

### > Data Product (csv flat file - Tim Marketing)
- Data disimpan dalam bentuk file .csv yang messy.
- Banyak nilai kosong/missing values.
- Format data tidak standar (contoh: harga pakai format string "USD 100" bukannya integer, ada koma/titik tidak konsisten).

### > Data NLP (Web Scrapping - Tim Data Scientist)
- Belum ada data sama sekali untuk riset NLP.
- Harus dilakukan proses web scraping untuk mengumpulkan teks dari website.
- Potensi masalah tambahan: data tidak terstruktur, format HTML bervariasi, kemungkinan adanya dirty data (stopwords, simbol, encoding error).

### > Data Infrastructure (Perusahaan XYZ masih baru)
- Belum ada sistem data pipeline yang robust.
- Belum ada database pusat (data warehouse atau staging DB) untuk menyimpan hasil ETL.
- Belum ada scheduling otomatis (pipeline harus bisa jalan rutin, misalnya harian).

## WORKFLOW DIAGRAM DATA ETL WITH LUIGI
![WORKFLOW DIAGRAM DATA ETL WITH LUIGI](img/flow_desing_data_engineering.png)

## Data Engineer Solution

### 1. Integrasi Multi-Source Data

- Problem: Data tersebar di beberapa sumber (PostgreSQL Sales DB, CSV Product Pricing, Web Scraping untuk NLP).

- Solution:
    - Gunakan ETL pipeline berbasis Luigi untuk  mengorkestrasi proses ETL per sumber data.

    - Buat task Extract khusus untuk tiap sumber:

    - ExtractSales (ambil data penjualan dari PostgreSQL).

    - ExtractProductCSV (baca file .csv dari tim produk).

    - ExtractScraping (ambil data teks hasil web scraping).

### 2. Data Cleaning & Transformation

- Problem: Data kotor, banyak missing values, format tidak konsisten.

- Solution:
    - Implementasikan task Transform per dataset:

    - Sales: isi nilai kosong, ubah format tanggal, hapus duplikasi.

    - Product Pricing (CSV): normalisasi format harga, isi nilai kosong dengan median/mean, standarisasi nama produk.

    - Scraping (Text): hilangkan HTML tag, simbol, karakter tidak relevan, normalisasi encoding.

### 3. Data Centralization

- Problem: Tidak ada satu database pusat.

- Solution:

    - Siapkan satu PostgreSQL database (etl_db) sebagai central repository.

    - Buat task Load untuk setiap dataset:

    - LoadSales → tabel sales_clean.

    - LoadProduct → tabel product_clean.

    - LoadScraping → tabel nlp_texts.


### 5. Monitoring & Logging

- Problem: Pipeline rawan gagal tanpa monitoring.

- Solution:

    - Implementasi custom logger (Python logging) agar tiap task Luigi punya log jelas.

    - Simpan log di folder logs/ untuk audit trail & debugging.

    - Luigi juga menyediakan UI Monitoring untuk memantau status task (success, failed, pending).

