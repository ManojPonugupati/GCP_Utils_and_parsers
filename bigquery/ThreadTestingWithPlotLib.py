import matplotlib.pyplot as plt
import time
from concurrent.futures import ThreadPoolExecutor


def example_task(n):
    """Example task to simulate workload."""
    time.sleep(2)  # Simulate a delay (I/O-bound task)


def test_thread_scaling(worker_counts, task_count):
    times = []
    for workers in worker_counts:
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(example_task, i) for i in range(task_count)]
            for future in futures:
                future.result()

        elapsed_time = time.time() - start_time
        times.append(elapsed_time)
        print(f"Workers: {workers}, Time: {elapsed_time:.2f} seconds")

    return times


# Define number of workers to test
worker_counts = [5, 10, 20, 30, 40, 50, 60, 80, 100]
task_count = 100  # Number of tasks

# Run the scaling test
times = test_thread_scaling(worker_counts, task_count)

# Plot the results
plt.plot(worker_counts, times, marker='o')
plt.xlabel('Number of Workers')
plt.ylabel('Execution Time (seconds)')
plt.title('Scaling Threads: Execution Time vs Number of Workers')
plt.show()
