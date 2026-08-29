import multiprocessing
import os
import sys
import time

APP_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if APP_ROOT not in sys.path:
    sys.path.insert(0, APP_ROOT)

from streaming.bronze_cdc import run_bronze
from streaming.silver_cdc import run_silver

def start_bronze_process():
    print("Starting Bronze CDC streaming process...")
    run_bronze()

def start_silver_process():
    print("Starting Silver CDC streaming process...")
    # Delay silver slightly to ensure bronze Delta table directory is initialized
    time.sleep(5)
    run_silver()

if __name__ == "__main__":
    p_bronze = multiprocessing.Process(target=start_bronze_process)
    p_silver = multiprocessing.Process(target=start_silver_process)

    p_bronze.start()
    p_silver.start()

    try:
        p_bronze.join()
        p_silver.join()
    except KeyboardInterrupt:
        print("\nStopping streaming pipelines...")
        p_bronze.terminate()
        p_silver.terminate()
        print("Pipeline processes terminated successfully.")