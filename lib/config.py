import os
from pathlib import Path

DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data"))

AN_BRONZE = DATA_ROOT / "AN" / "bronze"
AN_SILVER = DATA_ROOT / "AN" / "silver"
AN_GOLD = DATA_ROOT / "AN" / "gold"

EU_BRONZE = DATA_ROOT / "EU" / "bronze"
EU_SILVER = DATA_ROOT / "EU" / "silver"
EU_GOLD = DATA_ROOT / "EU" / "gold"

SNT_BRONZE = DATA_ROOT / "SNT" / "bronze"
SNT_SILVER = DATA_ROOT / "SNT" / "silver"
SNT_GOLD = DATA_ROOT / "SNT" / "gold"