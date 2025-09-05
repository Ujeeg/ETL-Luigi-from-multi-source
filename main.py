import luigi
import logging

# Import task yang sudah kamu buat
from tasks.extract_tasks.extract_task import ExtractSales, ExtractMarketing, ExtractBooks

if __name__ == "__main__":
    # Setup logging biar kelihatan jelas di terminal
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    print("=== Test Luigi Extract Tasks ===")

    # Jalankan task ExtractSales
    print("\n▶️ Menjalankan ExtractSales...")
    luigi.build([ExtractSales()], local_scheduler=True)

    # Jalankan task ExtractMarketing
    print("\n▶️ Menjalankan ExtractMarketing...")
    luigi.build([ExtractMarketing()], local_scheduler=True)

    # Jalankan task ExtractBooks
    print("\n▶️ Menjalankan ExtractBooks...")
    luigi.build([ExtractBooks()], local_scheduler=True)

    print("\n✅ Selesai test semua task.")
