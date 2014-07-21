import random
import time


max_attempts = 5


def attempt(f, *args, **kwargs):
    attempts = 0

    while True:
        try:
            return f(*args, **kwargs)
        except Exception as e:
            if getattr(e, "do_not_retry", False):
                raise

            attempts += 1

            if attempts >= max_attempts:
                raise

            sleep_time = random.uniform(0, (2 ** attempts) - 1)
            time.sleep(sleep_time)
