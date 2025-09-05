import luigi
import logging

# Import task yang sudah kamu buat
from tasks.transform_tasks.transform_task import TransformDataSales, TransformDataMarketng, TransformDataBooks

if __name__ == "__main__":
    # Setup logging biar kelihatan jelas di terminal
    print("=== Test Luigi Extract Tasks ===")

    # Jalankan task ExtractSales
    print("\n▶️ Menjalankan ExtractSales...")
    luigi.build([TransformDataSales()], local_scheduler=True)

    print("\n✅ Selesai test semua task.")
