import heapq
import collections
import random
import copy

class CacheLRU:
    def __init__(self, size):
        if size <= 0:
            raise ValueError("Cache size must be positive.")
        self.size = size
        self.content = set()
        self.usage_order = collections.deque()
        self.hits = 0
        self.misses = 0

    def access_block(self, block_id):
        if block_id in self.content:
            self.hits += 1
            self.usage_order.remove(block_id)
            self.usage_order.append(block_id)
            return True
        else:
            self.misses += 1
            self._add_block(block_id)
            return False

    def _add_block(self, block_id):
        if len(self.content) >= self.size:
            lru_block = self.usage_order.popleft()
            self.content.remove(lru_block)

        self.content.add(block_id)
        self.usage_order.append(block_id)

    def get_stats(self):
        return {"hits": self.hits, "misses": self.misses}

    def reset(self):
        self.content.clear()
        self.usage_order.clear()
        self.hits = 0
        self.misses = 0

class Task:
    def __init__(self, task_id, arrival_time, data_blocks):
        self.task_id = task_id
        self.arrival_time = arrival_time
        self.data_blocks = set(data_blocks)
        self.remaining_blocks = list(data_blocks)
        self.start_time = -1
        self.completion_time = -1
        self.wait_time = 0

    def __lt__(self, other):
        if self.arrival_time != other.arrival_time:
            return self.arrival_time < other.arrival_time
        return self.task_id < other.task_id


    def __repr__(self):
        return (f"Task(id={self.task_id}, arrival={self.arrival_time}, "
                f"blocks={sorted(list(self.data_blocks))})")

class BaseScheduler:
    def __init__(self, cache_size):
        self.cache = CacheLRU(cache_size)
        self.tasks_to_schedule = []
        self.completed_tasks = []
        self.current_time = 0

    def add_task(self, task):
        self.tasks_to_schedule.append(task)

    def run_simulation(self):
        raise NotImplementedError("Subclasses must implement run_simulation")

    def _run_task_step(self, task):
        if not task.remaining_blocks:
            return True

        if task.start_time == -1:
             task.start_time = self.current_time
             task.wait_time = task.start_time - task.arrival_time

        block_to_access = task.remaining_blocks.pop(0)
        self.cache.current_time = self.current_time
        self.cache.access_block(block_to_access)

        if not task.remaining_blocks:
            task.completion_time = self.current_time + 1
            return True
        return False

    def print_results(self, policy_name):
        print(f"\n--- Results for {policy_name} Scheduler ---")
        cache_stats = self.cache.get_stats()
        total_accesses = cache_stats['hits'] + cache_stats['misses']
        hit_rate = (cache_stats['hits'] / total_accesses * 100) if total_accesses > 0 else 0
        miss_rate = (cache_stats['misses'] / total_accesses * 100) if total_accesses > 0 else 0

        final_completion_time = 0
        if self.completed_tasks:
             final_completion_time = max(t.completion_time for t in self.completed_tasks)
        else:
             final_completion_time = self.current_time

        print(f"Cache Size: {self.cache.size}")
        print(f"Total Simulation Time: {final_completion_time}")
        print(f"Total Block Accesses: {total_accesses}")
        print(f"Cache Hits: {cache_stats['hits']}")
        print(f"Cache Misses: {cache_stats['misses']}")
        print(f"Cache Hit Rate: {hit_rate:.2f}%")
        print(f"Cache Miss Rate: {miss_rate:.2f}%")

        num_completed = len(self.completed_tasks)
        if num_completed > 0:
            avg_wait = sum(t.wait_time for t in self.completed_tasks) / num_completed
            avg_turnaround = sum(t.completion_time - t.arrival_time for t in self.completed_tasks) / num_completed
            print(f"Average Wait Time: {avg_wait:.2f}")
            print(f"Average Turnaround Time: {avg_turnaround:.2f}")
        else:
            print("No tasks completed.")
        print("----------------------------------------")


class FIFOScheduler(BaseScheduler):
    def run_simulation(self):
        self.cache.reset()
        self.completed_tasks = []
        ready_queue = collections.deque()
        task_pool = sorted(self.tasks_to_schedule, key=lambda t: t.arrival_time)
        current_task = None
        self.current_time = 0
        task_pool_idx = 0

        print(f"Starting FIFO Simulation. Total tasks: {len(task_pool)}")

        while task_pool_idx < len(task_pool) or ready_queue or current_task:
            time_jumped = False

            while task_pool_idx < len(task_pool) and task_pool[task_pool_idx].arrival_time <= self.current_time:
                arriving_task = task_pool[task_pool_idx]
                ready_queue.append(arriving_task)
                task_pool_idx += 1

            if current_task is None and ready_queue:
                current_task = ready_queue.popleft()

            if current_task is not None:
                task_finished = self._run_task_step(current_task)
                if task_finished:
                    self.completed_tasks.append(current_task)
                    current_task = None

            if current_task is None and not ready_queue and task_pool_idx < len(task_pool):
                next_arrival_time = task_pool[task_pool_idx].arrival_time
                if next_arrival_time > self.current_time:
                    self.current_time = next_arrival_time
                    time_jumped = True

            if not time_jumped:
                self.current_time += 1

            if not (task_pool_idx < len(task_pool) or ready_queue or current_task):
                break

        print(f"FIFO Simulation Finished.")


class CacheAwareScheduler(BaseScheduler):
    def run_simulation(self):
        self.cache.reset()
        self.completed_tasks = []
        ready_priority_queue = []
        task_pool = sorted(self.tasks_to_schedule, key=lambda t: t.arrival_time)
        current_task = None
        self.current_time = 0
        task_pool_idx = 0
        ready_task_ids = set()

        print(f"Starting Cache-Aware Simulation. Total tasks: {len(task_pool)}")

        while task_pool_idx < len(task_pool) or ready_priority_queue or current_task:
            time_jumped = False
            cache_state_changed = False

            newly_arrived_tasks = []
            while task_pool_idx < len(task_pool) and task_pool[task_pool_idx].arrival_time <= self.current_time:
                arriving_task = task_pool[task_pool_idx]
                if arriving_task.task_id not in ready_task_ids:
                     newly_arrived_tasks.append(arriving_task)
                     ready_task_ids.add(arriving_task.task_id)
                task_pool_idx += 1

            for task in newly_arrived_tasks:
                potential_hits = len(task.data_blocks.intersection(self.cache.content))
                priority_tuple = (-potential_hits, task.arrival_time, task)
                heapq.heappush(ready_priority_queue, priority_tuple)

            if current_task is None and ready_priority_queue:
                 current_ready_items = [item[2] for item in ready_priority_queue]
                 ready_priority_queue = []
                 ready_task_ids.clear()

                 for task in current_ready_items:
                     potential_hits = len(task.data_blocks.intersection(self.cache.content))
                     new_priority_tuple = (-potential_hits, task.arrival_time, task)
                     heapq.heappush(ready_priority_queue, new_priority_tuple)
                     ready_task_ids.add(task.task_id)

                 if ready_priority_queue:
                     neg_hits, arrival, current_task = heapq.heappop(ready_priority_queue)
                     ready_task_ids.remove(current_task.task_id)

            if current_task is not None:
                task_finished = self._run_task_step(current_task)
                if task_finished:
                    self.completed_tasks.append(current_task)
                    current_task = None
                    cache_state_changed = True

            if current_task is None and not ready_priority_queue and task_pool_idx < len(task_pool):
                next_arrival_time = task_pool[task_pool_idx].arrival_time
                if next_arrival_time > self.current_time:
                    self.current_time = next_arrival_time
                    time_jumped = True

            if not time_jumped:
                self.current_time += 1

            if not (task_pool_idx < len(task_pool) or ready_priority_queue or current_task):
                break

        print(f"Cache-Aware Simulation Finished.")


def generate_tasks(num_tasks, max_arrival_time, max_blocks_per_task, data_block_range, locality_factor=0.4):
    tasks = []
    all_blocks = list(range(data_block_range))
    if data_block_range <= 0 or max_blocks_per_task <= 0:
        print("Warning: data_block_range or max_blocks_per_task is zero or negative.")
        return []

    last_task_blocks = set()

    for i in range(num_tasks):
        arrival = random.randint(0, max_arrival_time)
        num_blocks = random.randint(1, min(max_blocks_per_task, data_block_range))
        data_blocks = set()

        if last_task_blocks and random.random() < locality_factor:
            k_reuse = min(random.randint(1, num_blocks // 2 + 1), len(last_task_blocks))
            if k_reuse > 0:
                 actual_k_reuse = min(k_reuse, len(last_task_blocks))
                 data_blocks.update(random.sample(list(last_task_blocks), actual_k_reuse))

        needed_new = num_blocks - len(data_blocks)
        if needed_new > 0:
            available_new = [b for b in all_blocks if b not in data_blocks]
            k_new = min(needed_new, len(available_new))
            if k_new > 0:
                 data_blocks.update(random.sample(available_new, k_new))

        fill_attempts = 0
        while len(data_blocks) < num_blocks and fill_attempts < 5:
             needed_fill = num_blocks - len(data_blocks)
             available_fill = [b for b in all_blocks if b not in data_blocks]
             k_fill = min(needed_fill, len(available_fill))
             if k_fill <=0: break
             data_blocks.update(random.sample(available_fill, k_fill))
             fill_attempts += 1

        if not data_blocks:
             if all_blocks:
                 data_blocks.add(random.choice(all_blocks))
             else:
                 print(f"Warning: Cannot assign blocks to task {i}, data_block_range is likely 0.")

        new_task = Task(task_id=i, arrival_time=arrival, data_blocks=list(data_blocks))
        tasks.append(new_task)
        last_task_blocks = new_task.data_blocks

    print(f"Generated {len(tasks)} tasks.")
    return tasks

if __name__ == "__main__":
    CACHE_SIZE = 16
    NUM_TASKS = 50
    MAX_ARRIVAL = 100
    MAX_BLOCKS_PER_TASK = 25
    DATA_BLOCK_RANGE = 64
    LOCALITY_FACTOR = 0.5
    RANDOM_SEED = 42

    print("--- Starting Cache-Aware Scheduler Simulation ---")
    print(f"Parameters: Cache Size={CACHE_SIZE}, Tasks={NUM_TASKS}, Max Arrival={MAX_ARRIVAL}, "
          f"Max Blocks/Task={MAX_BLOCKS_PER_TASK}, Block Range={DATA_BLOCK_RANGE}, "
          f"Locality Factor={LOCALITY_FACTOR}, Seed={RANDOM_SEED}")

    random.seed(RANDOM_SEED)
    sim_tasks = generate_tasks(NUM_TASKS, MAX_ARRIVAL, MAX_BLOCKS_PER_TASK, DATA_BLOCK_RANGE, LOCALITY_FACTOR)

    if not sim_tasks:
        print("\nNo tasks generated. Exiting.")
    else:
        fifo_tasks = [copy.deepcopy(t) for t in sim_tasks]
        fifo_scheduler = FIFOScheduler(CACHE_SIZE)
        for task in fifo_tasks:
            fifo_scheduler.add_task(task)
        print("\nRunning FIFO Simulation...")
        fifo_scheduler.run_simulation()
        fifo_scheduler.print_results("FIFO")

        cache_aware_tasks = [copy.deepcopy(t) for t in sim_tasks]
        cache_aware_scheduler = CacheAwareScheduler(CACHE_SIZE)
        for task in cache_aware_tasks:
            cache_aware_scheduler.add_task(task)
        print("\nRunning Cache-Aware Simulation...")
        cache_aware_scheduler.run_simulation()
        cache_aware_scheduler.print_results("Cache-Aware (Priority Queue)")

        print("\n--- Simulation Complete ---")

