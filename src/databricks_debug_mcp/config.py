import os
from dataclasses import dataclass, field


@dataclass
class Config:
    profile: str = field(default_factory=lambda: os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT"))
    log_tail_default: int = 200


_config: Config | None = None


def get_config() -> Config:
    global _config
    if _config is None:
        _config = Config()
    return _config
