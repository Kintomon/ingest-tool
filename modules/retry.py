"""Retry logic with exponential backoff"""

import time
import logging
from typing import Callable, TypeVar, Optional, Tuple
from functools import wraps

T = TypeVar('T')
logger = logging.getLogger(__name__)


def retry_with_backoff(
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: Tuple[Exception, ...] = (Exception,)
):
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            delay = initial_delay
            last_exception = None
            
            for attempt in range(3 + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < 3:
                        logger.warning(
                            f"Attempt {attempt + 1}/{3 + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                        delay = min(delay * exponential_base, max_delay)
                    else:
                        logger.error(f"All {3 + 1} attempts failed for {func.__name__}")
                        raise
            
            if last_exception:
                raise last_exception
            raise RuntimeError("Unexpected error in retry logic")
        
        return wrapper
    return decorator


def validate_jwt_token(token: str) -> bool:
    if not token or not isinstance(token, str):
        return False
    
    parts = token.split('.')
    if len(parts) != 3:
        return False
    
    if not all(part for part in parts):
        return False
    
    if len(token) < 50:
        return False
    
    return True

