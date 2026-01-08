# iFlags (interceptor flags)

`iFlags` is a bitmap for version + best-effort error conditions. The interceptor should never disrupt the caller.

## Bit definitions (Python)

| Name | Value | Meaning |
|---|---:|---|
| `IVER8` | `1` | Version marker (always set). |
| `PY_DRIVER_PYMYSQL` | `2` | PyMySQL driver marker (distinguish from Java v8). |
| `PY_DRIVER_SQLALCHEMY` | `4` | SQLAlchemy driver marker (distinguish from Java v8). |
| `PY_ERROR_CONNECTION_ID` | `2097152` | Error fetching `connectionId`. |
| `PY_ERROR_PREPROCESS_BATCHED_ARGS` | `4194304` | Error recording params for `executemany` pre-exec. |
| `PY_ERROR_POSTPROCESS_BATCHED_ARGS` | `8388608` | Error serializing params into `queryParams`. |
| `PY_ERROR_DEFAULT_TZ` | `16777216` | Error determining client `defaultTZ`. |
| `PY_ERROR_SERVER_TZ` | `33554432` | Error determining server `serverTZ`. |
| `PY_ERROR_ISOLATION` | `67108864` | Error determining `isolationLvl`. |
| `PY_ERROR_CLIENT_FLAGS` | `134217728` | Error determining `clientFlags`. |
| `PY_ERROR_SERVER_FLAGS` | `268435456` | Error determining `serverFlags`. |
| `PY_ERROR_SERVER_VERSION` | `536870912` | Error determining `serverVersion`. |
| `PY_ERROR_SERVER_HOST` | `1073741824` | Error determining `serverHost`. |
| `PY_ERROR_SERVER_INFO` | `2147483648` | Exception during `serverInfo` extraction. |

## Isolation level encoding (`isolationLvl`)

| MySQL `@@transaction_isolation` | `isolationLvl` |
|---|---:|
| `READ-UNCOMMITTED` | `1` |
| `READ-COMMITTED` | `2` |
| `REPEATABLE-READ` | `4` |
| `SERIALIZABLE` | `8` |
