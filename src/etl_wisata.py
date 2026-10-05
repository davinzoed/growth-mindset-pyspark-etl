"""
ETL sederhana: Jumlah Wisatawan per Tempat Wisata di Kabupaten Mojokerto pada Tahun 2025

Alur:
    extract  -> baca CSV (semua kolom sebagai teks), cast kolom total dan tahun ke int
    transform-> saring tahun 2025, tambah rata_per_bulan dan kategori, urutkan
    load     -> simpan hasil ke Parquet
    validate -> baca ulang hasil dan cek jumlah baris, keunikan kode, dan kategori

Catatan: path Volume di bawah perlu disesuaikan dengan workspace masing-masing (saya menggunakan Databricks maka storagenya menggunakan Unity Catalog milik databricks (cloud)).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/Volumes/kuliah_pens/pengembangan_karakter_davin/dataset/"
SUMBER = f"{BASE}/jumlah-wisatawan-tahun-n-2025.csv"
OUTPUT = f"{BASE}/output/wisata_ringkas"

BATAS_TINGGI = 20000   # rata_per_bulan di atas ini = Tinggi
BATAS_SEDANG = 10000   # rata_per_bulan mulai dari ini = Sedang

# Di Databricks, `spark` sudah tersedia. Baris ini membuat skrip tetap bisa dijalankan di tempat lain.
spark = SparkSession.builder.getOrCreate()

def extract(path):
    # Tanpa inferSchema supaya kode_wisata (misalnya "1.10") tetap berupa teks.
    df = spark.read.csv(path, header=True)
    return (df.withColumn("total", F.col("total").cast("int"))
              .withColumn("tahun", F.col("tahun").cast("int")))

def transform(df):
    df = df.filter(F.col("tahun") == 2025)
    return (df
        .withColumn("rata_per_bulan", F.round(F.col("total") / 12))
        .withColumn("kategori",
                    F.when(F.col("rata_per_bulan") > BATAS_TINGGI, "Tinggi")
                     .when(F.col("rata_per_bulan") >= BATAS_SEDANG, "Sedang")
                     .otherwise("Rendah"))
        .select("kode_wisata", "nama_wisata", "total", "rata_per_bulan", "kategori")
        .orderBy(F.col("total").desc()))

def load(df, path):
    df.write.mode("overwrite").parquet(path)

def validate(path):
    hasil = spark.read.parquet(path)                # baca ulang dari output
    assert hasil.count() == 12, "Jumlah baris hasil tidak sesuai."
    assert hasil.select("kode_wisata").distinct().count() == 12, "Kode wisata tidak unik."
    assert hasil.filter(F.col("kategori").isNull()).count() == 0, "Ada kategori kosong."
    print("Validasi lolos.")

if __name__ == "__main__":
    raw = extract(SUMBER)
    hasil = transform(raw)
    load(hasil, OUTPUT)
    validate(OUTPUT)
    hasil.show(truncate=False)
