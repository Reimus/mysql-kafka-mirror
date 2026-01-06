from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class SqlLogMessage:
    timestamp: int  # epoch millis (wall clock)
    serverHost: Optional[str]
    serverVersion: Optional[str]
    user: Optional[str]
    client: Optional[str]
    dbName: Optional[str]
    stmtDbName: Optional[str]
    debug: Optional[str]
    connectionId: Optional[int]
    totalPoolCount: Optional[int]
    executionCount: Optional[int]
    serverFlags: Optional[int]
    clientFlags: Optional[int]
    iFlags: int
    defaultTZ: Optional[str]
    serverTZ: Optional[str]
    isolationLvl: Optional[int]
    durationNs: Optional[int]
    updateCount: Optional[int]
    sql: Optional[str]
    queryParams: Optional[List[str]]
    errorMessage: Optional[str]
    serverInfo: Optional[str]

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)
