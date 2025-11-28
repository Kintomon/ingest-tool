"""Configuration management with environment variable and file support"""

import os
import yaml
from pathlib import Path
from typing import Optional, Dict, Any


class Config:
    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or "config.yaml"
        self._config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self):
        config_path = Path(self.config_file)
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    self._config = yaml.safe_load(f) or {}
            except Exception as e:
                print(f"⚠️  Warning: Could not load config file: {e}")
                self._config = {}
        else:
            self._config = {}
    
    def get(self, key: str, default: Any = None, env_var: Optional[str] = None) -> Any:
        env_key = env_var or key.upper().replace('.', '_')
        env_value = os.getenv(env_key)
        if env_value is not None:
            return env_value
        
        if '.' in key:
            keys = key.split('.')
            value = self._config
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k)
                else:
                    value = None
                    break
            if value is not None:
                return value
        else:
            if key in self._config:
                return self._config[key]
        
        return default
    
    def get_bool(self, key: str, default: bool = False, env_var: Optional[str] = None) -> bool:
        value = self.get(key, default, env_var)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value)
    
    def get_int(self, key: str, default: int = 0, env_var: Optional[str] = None) -> int:
        value = self.get(key, default, env_var)
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def get_float(self, key: str, default: float = 0.0, env_var: Optional[str] = None) -> float:
        value = self.get(key, default, env_var)
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

