import luigi
import logging

# Import task yang sudah kamu buat
from tasks.load_task.load_task import (
    LoadDataSales,
    LoadDataMarketing,
    LoadDataBooks
)

if __name__ == "__main__":
    # Setup logging biar kelihatan jelas di terminal
    logging.basicConfig(level=logging.INFO)
    print("=== Mulai menjalankan semua pipeline ETL (Sales, Marketing, Books) ===")

    # Jalankan semua task Load (otomatis akan jalan Extract + Transform lebih dulu)
    luigi.build(
        [
            LoadDataSales(),
            LoadDataMarketing(),
            LoadDataBooks()
        ],
        local_scheduler=True,
        workers=3  # bisa paralel 3 worker
    )

    print("\n✅ Semua task ETL selesai dijalankan.")
