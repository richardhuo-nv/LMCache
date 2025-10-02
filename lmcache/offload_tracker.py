import threading
import time

class OffloadTracker:
    def __init__(self, file_path: str, interval: int = 10, reset_after_write: bool = False):
        """
        :param file_path: File path to write the counts to.
        :param interval: Write interval in seconds.
        :param reset_after_write: If True, counters reset to 0 after each file write.
        """
        self.file_path = file_path
        self.interval = interval
        self.reset_after_write = reset_after_write

        self._count = 0
        self._bytes = 0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        self._thread = threading.Thread(target=self._writer_thread, daemon=True)
        self._thread.start()

    def increment(self, n_events: int = 1, n_bytes: int = 0):
        """Increment the offload counters."""
        with self._lock:
            self._count += n_events
            self._bytes += n_bytes

    def get_snapshot(self):
        """Get a snapshot of the current counters (thread-safe)."""
        with self._lock:
            return self._count, self._bytes

    def _reset_counters(self):
        with self._lock:
            self._count = 0
            self._bytes = 0

    def _writer_thread(self):
        """Background thread that writes counters to file every interval seconds."""
        while not self._stop_event.is_set():
            time.sleep(self.interval)
            count, bytes_ = self.get_snapshot()
            try:
                with open(self.file_path, "a") as f:
                    f.write(
                        f"{time.strftime('%Y-%m-%d %H:%M:%S')} - "
                        f"Host->Disk offloads: {count} events, {bytes_} bytes\n"
                    )
            except Exception as e:
                print(f"Error writing to file: {e}")

            if self.reset_after_write:
                self._reset_counters()

    def stop(self):
        """Stop the background writer thread."""
        self._stop_event.set()
        self._thread.join()
