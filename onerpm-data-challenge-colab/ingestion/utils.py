from time import perf_counter


class Timer:

    def __enter__(self):

        self.start = perf_counter()

        return self

    def __exit__(self, *args):

        self.end = perf_counter()

        print(f"Tempo: {self.end-self.start:.2f}s")