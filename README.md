# 7 Hari Mengubah Cara Belajar: ETL PySpark Sederhana (Data Wisata Kab. Mojokerto)

Repositori ini adalah pelengkap laporan Tugas Individu "Misi Perubahan Paradigma"
(D4 Sains Data Terapan, PENS).

Nama  : Davin Zoe Akhmad
NRP   : 3326600003
Prodi : Sains Data Terapan A (2026)

## Isi
- `notebooks/` : notebook latihan eksperimen Hari 1-4 dan Hari 6 (dijalankan di
  Databricks Free Edition, serverless).
- `src/etl_wisata.py` : skrip ETL final (extract, transform, load, validate).

## Dataset
Jumlah Wisatawan per Tempat Wisata di Kabupaten Mojokerto Tahun 2025
Sumber: https://satudata.mojokertokab.go.id/dataset_resource/f29df43f-6a63-46ff-a94f-9ee15fd854c6

## Ringkasan ETL
- Extract : baca CSV tanpa inferSchema, lalu cast kolom total dan tahun ke integer.
- Transform : saring tahun 2025, tambah rata_per_bulan dan kategori
  (Tinggi > 20.000, Sedang >= 10.000, selain itu Rendah), urutkan berdasarkan total.
- Load : simpan ke Parquet di Unity Catalog Volume.
- Validate : cek jumlah baris, keunikan kode_wisata, dan kategori tidak kosong.

## Catatan
Path Volume di skrip perlu disesuaikan dengan workspace masing-masing.
