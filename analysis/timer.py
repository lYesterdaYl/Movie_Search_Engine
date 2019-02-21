import timeit


class Timer:

    def __init__(self, task_size):
        self.start = timeit.default_timer()
        self.stop = timeit.default_timer()
        self.task_size = task_size
        self.seed = 0

    def increment_seed(self):
        self.seed += 1

    def print_progress(self):
        self.stop = timeit.default_timer()
        print("Initialize-Index-Progress: ", round(self.seed / self.task_size * 100, 2), "%", "   Used Time: ",
              round(self.stop - self.start, 2), "   Estimate Remain Time: ",
              round(100 / (self.seed / self.task_size * 100) * (self.stop - self.start) - (self.stop - self.start), 2),
              " seconds")
