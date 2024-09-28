from concurrent.futures import ThreadPoolExecutor
import time

def example_task(n):
    """Example task function, replace with actual task like querying BigQuery"""
    time.sleep(2)  # Simulate task delay
    return n

def test_max_workers(worker_counts, task_count):
    """Test task completion time for different worker counts."""
    for workers in worker_counts:
        print(f"Testing with {workers} workers:")
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(example_task, i) for i in range(task_count)]
            for future in futures:
                future.result()

        elapsed_time = time.time() - start_time
        print(f"Completed in {elapsed_time:.2f} seconds with {workers} workers.\n")

# Example usage: testing different worker counts
worker_counts_to_test = [5, 10, 20, 50,100,200,400,800]  # Adjust based on your scenario
task_count = 100  # Number of tasks to test
test_max_workers(worker_counts_to_test, task_count)
