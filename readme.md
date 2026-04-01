# ILDKI Data Pipeline: Prefect + dbt + StarRocks

Proyek ini adalah implementasi *Modern Data Stack* (MDS) untuk kebutuhan ILDKI, yang mengintegrasikan **Prefect** sebagai workflow orchestrator dan **dbt (data build tool)** sebagai lapisan transformasi data, dengan **StarRocks** sebagai High-Performance OLAP database.

## Arsitektur

1.  **Orchestration (Prefect):** Mengatur jadwal (scheduling), monitoring, dan eksekusi flow kerja. Menggunakan library `prefect-dbt` untuk memicu perintah dbt secara terprogram.
2.  **Transformation (dbt):** Mengelola logika bisnis dan transformasi data menggunakan SQL. Proyek ini menggunakan adapter `dbt-starrocks` untuk berkomunikasi dengan engine StarRocks.
3.  **Storage/OLAP (StarRocks):** Berfungsi sebagai target data warehouse tempat data hasil transformasi disimpan dan siap dianalisis.

## 📂 Struktur Proyek & Fungsi File

Proyek ini menggunakan pemisahan ketat antara **Infrastruktur** (Python/K8s) dan **Logika Bisnis** (SQL).

```text

├── prefect.yaml          # Konfigurasi Deployment Prefect (Jadwal & Resource K8s)
├── run_dbt.py            # Entrypoint Python (General Runner - JANGAN DIUBAH)
├── .dockerignore         # Mencegah file sampah masuk ke container K8s
└── dbt_project/          # FOLDER UTAMA UNTUK PROGRAMMER SQL
    ├── dbt_project.yml   # Konfigurasi utama dbt (Nama Project, Folder Model)
    ├── profiles.yml      # Konfigurasi koneksi ke StarRocks (Host, User, Pass)
    ├── packages.yml      # Library tambahan dbt
    ├── models/           # TEMPAT MENULIS QUERY SQL
    │   ├── staging/      # Layer 1: Pembersihan data mentah (Rename, Casting)
    │   └── marts/        # Layer 2: Logika Bisnis (Join, Aggregation, Fact/Dim)
    ├── seeds/            # Data statis .csv (misal: Kode Wilayah, Master Category)
    ├── tests/            # Custom Data Quality Tests
    └── macros/           # Fungsi SQL reusable (Jinja2 templates)
```
    
## Prasyarat

Pastikan Anda telah menginstal dependensi berikut:

```bash
pip install prefect prefect-dbt dbt-starrocks
```

## Konfigurasi

1.  **dbt Profile:** Pastikan file `profiles.yml` Anda dikonfigurasi untuk menggunakan type `starrocks`.
2.  **Prefect Blocks:** Buat `DbtCloudCredentials` atau `DbtCliProfile` di dashboard Prefect untuk menghubungkan flow dengan instalasi dbt Anda.

## Cara Menjalankan

Untuk menjalankan transformasi melalui Prefect, Anda dapat mengeksekusi script flow yang memanggil task dbt:

```python
from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation

@flow
def run_dbt_transformation():
    result = DbtCoreOperation(
        commands=["dbt run"],
        project_dir="dbt",
        profiles_dir="."
    ).run()
    return result
```

## Dokumentasi Terkait
- Prefect-dbt Documentation
- dbt-starrocks Adapter Guide