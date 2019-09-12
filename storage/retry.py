import logging
import random
import time

from typing import Any, Callable, TypeVar


max_attempts: int = 5


T = TypeVar("T")


def attempt(f: Callable[..., T], *args: Any, **kwargs: Any) -> T:
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

            logging.warning(f"Retry attempt #{attempts}", exc_info=True)
