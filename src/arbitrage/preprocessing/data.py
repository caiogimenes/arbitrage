from dataclasses import dataclass

@dataclass
class StockInfo:
    close: str = None
    timestamp: str = None