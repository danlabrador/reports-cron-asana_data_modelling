import time
import threading

class Throttler:
    """
    A simple throttler that limits the number of API requests within a given time window.
    
    For example, to allow 10 requests per 60 seconds:
    
        throttler = Throttler(rate_limit=10, period=60)
    """
    def __init__(self, rate_limit: int, period: int):
        self.rate_limit = rate_limit
        self.period = period
        self.lock = threading.Lock()
        self.requests = []

    def acquire(self):
        with self.lock:
            current_time = time.time()
            # Remove timestamps that are older than the period
            self.requests = [t for t in self.requests if current_time - t < self.period]

            if len(self.requests) >= self.rate_limit:
                # Calculate wait time until the earliest request is outside the time window.
                wait_time = self.period - (current_time - self.requests[0])
                time.sleep(wait_time)
                # After sleeping, clean up the list.
                current_time = time.time()
                self.requests = [t for t in self.requests if current_time - t < self.period]
            
            # Record the current request timestamp.
            self.requests.append(time.time())
