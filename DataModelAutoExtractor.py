# -*- coding: utf-8 -*-
"""
Schema Exporter - A tool for exporting database schemas to CSV files for data governance

This script extracts database schema information from SQL Server/Microsoft Fabric databases
and exports it to CSV files, maintaining versioning and history. It supports various
authentication methods including service principal and interactive Azure AD authentication.

**Improvements incorporated:**
- File locking for versions.json to prevent race conditions.
- Database name used as base key for versioning (decoupled from date).
- Increased version number padding (_v001).
- Configurable archive keep count.
- Robust file operations (write to temp, rename).
- Basic validation for user input CSV files.
- Specific output columns forced to NULL as requested.
- Notification changed to a single summary message per domain run, sent via
  an HTTP POST request to a Power Automate Flow trigger at the end of
  processing for each domain.
- Requires 'requests' library for HTTP notifications.
- Includes basic SharePoint link generation (REQUIRES USER REVIEW AND CONFIGURATION).
"""

import argparse
import configparser
import json
import logging
import os
import shutil
import struct  # For interactive auth token structure
import sys
import time
import urllib.parse  # For URL encoding
import webbrowser
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Platform-specific locking (simple file-based lock as fallback)
try:
    import fcntl  # Unix

    LOCK_METHOD = "fcntl"
except ImportError:
    try:
        import msvcrt  # Windows

        LOCK_METHOD = "msvcrt"
    except ImportError:
        LOCK_METHOD = "file"  # Fallback simple lock file

# Third-party imports
try:
    import msal
    import pandas as pd
    import pyodbc
    import requests  # Required for HTTP notification
except ImportError as e:
    print(f"Error: Missing required packages. Please install dependencies. {e}")
    print("Try: pip install pyodbc pandas msal requests")
    sys.exit(1)

# ========== CONSTANTS ==========

ENCODING_UTF8_SIG = "utf-8-sig"
KEY_COLS = ["TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME"]
# Keep only columns that might contain manual data to be truly preserved across runs
PRESERVED_COLS = [
    "CONSTRAINT_TYPE",
    "FOREIGN_SCHEMA",
    "FOREIGN_TABLE",
    "FOREIGN_COLUMN",
    "BUSINESS_MEANING",
]
SUBFOLDERS = [
    "001 Archive",
    "002 SQL Output",
    "003 User Input",
    "004 Lucidchart ERD Diagram",
    "005 DDL",
]
VERSION_FILE_NAME = "versions.json"
VERSION_LOCK_FILE_NAME_SUFFIX = ".lock"  # Just the suffix
VERSION_BACKUP_FILE_NAME_SUFFIX = ".bak" # Just the suffix
DEFAULT_SENSITIVE_FLAG = 1  # Default value if not specified in config (though IS_SENSITIVE is now forced NULL)
DEFAULT_ARCHIVE_KEEP_COUNT = 5  # Default number of archives to keep
VERSION_PADDING = 3  # e.g., _v001, _v002, ...

# SQL query - MODIFIED to force certain columns to NULL
# No parameters needed anymore for this version
SQL_QUERY = """
WITH user_input AS (
    SELECT 1 as dummy -- Keep CTE valid if needed syntactically elsewhere, not used here
)
SELECT
    'sqlserver' AS dbms,
    t.TABLE_CATALOG,
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    c.COLUMN_NAME,
    c.ORDINAL_POSITION,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    n.CONSTRAINT_TYPE,
    k2.TABLE_SCHEMA AS FOREIGN_SCHEMA,
    k2.TABLE_NAME AS FOREIGN_TABLE,
    k2.COLUMN_NAME AS FOREIGN_COLUMN,
    CAST(NULL AS VARCHAR(MAX)) AS BUSINESS_MEANING,     -- Still preserved by merge logic if needed
    CAST(NULL AS VARCHAR(MAX)) AS BUSINESS_DOMAIN,        -- Forced NULL
    CAST(NULL AS VARCHAR(MAX)) AS BUSINESS_OWNER_NAME,    -- Forced NULL
    CAST(NULL AS INT) AS IS_SENSITIVE,                    -- Forced NULL
    CAST(NULL AS VARCHAR(100)) AS DATA_CLASSIFICATION,    -- Forced NULL
    CAST(NULL AS VARCHAR(MAX)) AS RELATIONSHIP_NOTE,      -- Forced NULL
    CAST(NULL AS VARCHAR(MAX)) AS SOURCE_NAME,            -- Forced NULL
    CAST(NULL AS VARCHAR(MAX)) AS CREATED_BY,             -- Forced NULL
    GETDATE() AS CREATED_AT                               -- Capture timestamp
FROM
    INFORMATION_SCHEMA.TABLES t
LEFT JOIN
    INFORMATION_SCHEMA.COLUMNS c
    ON t.TABLE_CATALOG = c.TABLE_CATALOG
    AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
    AND t.TABLE_NAME = c.TABLE_NAME
LEFT JOIN ( -- Join to get constraints (Primary Key, Foreign Key, Unique)
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS n
        ON k.CONSTRAINT_CATALOG = n.CONSTRAINT_CATALOG
        AND k.CONSTRAINT_SCHEMA = n.CONSTRAINT_SCHEMA
        AND k.CONSTRAINT_NAME = n.CONSTRAINT_NAME
    LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r -- Only populated for Foreign Keys
        ON k.CONSTRAINT_CATALOG = r.CONSTRAINT_CATALOG
        AND k.CONSTRAINT_SCHEMA = r.CONSTRAINT_SCHEMA
        AND k.CONSTRAINT_NAME = r.CONSTRAINT_NAME
) ON c.TABLE_CATALOG = k.TABLE_CATALOG
    AND c.TABLE_SCHEMA = k.TABLE_SCHEMA
    AND c.TABLE_NAME = k.TABLE_NAME
    AND c.COLUMN_NAME = k.COLUMN_NAME
LEFT JOIN
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE k2 -- Join again to get the referenced table/column for FKs
    ON k.ORDINAL_POSITION = k2.ORDINAL_POSITION -- Matches columns in multi-column FKs
    AND r.UNIQUE_CONSTRAINT_CATALOG = k2.CONSTRAINT_CATALOG -- Matches the referenced constraint
    AND r.UNIQUE_CONSTRAINT_SCHEMA = k2.CONSTRAINT_SCHEMA
    AND r.UNIQUE_CONSTRAINT_NAME = k2.CONSTRAINT_NAME
WHERE
    t.TABLE_TYPE = 'BASE TABLE'; -- Exclude VIEWS etc.
"""


# ========== DATA CLASSES FOR CONFIGURATION ==========
@dataclass
class DomainConfig:
    """Configuration for a specific data domain."""

    name: str
    sql_server: str
    sharepoint_path: Path  # Local/Network path to the root sync folder for this domain
    # --- NEW: Store base SP URL parts for link generation ---
    sharepoint_site_url: Optional[
        str
    ] = None  # e.g., https://yourtenant.sharepoint.com/sites/YourSite
    sharepoint_doc_library_path: Optional[
        str
    ] = None  # e.g., Shared%20Documents (URL encoded path segment)
    # --- End New ---
    db_prefix: Optional[str] = None
    db_override: List[str] = field(default_factory=list)


@dataclass
class AuthConfig:
    """Database authentication configuration."""

    method: str
    username: Optional[str] = None
    password: Optional[str] = None
    client_id: Optional[str] = None
    tenant_id: Optional[str] = None
    client_secret: Optional[str] = None


@dataclass
class UserInputConfig:
    """Default user input values from configuration."""

    created_by: str  # Used for 'triggeredBy' field in notification
    source_name: str  # Still used for logging/identification internally
    business_domain: str  # Not used in output CSV anymore
    business_owner: str  # Not used in output CSV anymore
    is_sensitive: int  # Not used in output CSV anymore
    archive_keep_count: int  # Configurable archive retention


# ========== LOGGING SETUP ==========
def setup_logging(log_level=logging.INFO) -> logging.Logger:
    """Configures logging to both file and console."""
    log_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)-15s: %(message)s"
    )
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"schema_extract_{datetime.now():%Y%m%d_%H%M%S}.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    try:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Could not set up file logging to {log_file}: {e}")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    # Set higher level for noisy libraries
    logging.getLogger("msal").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("pyodbc").setLevel(logging.INFO)

    return logging.getLogger(__name__)


logger = setup_logging()


# ========== AUTHENTICATION HELPERS ==========
def get_service_principal_token(auth_cfg: AuthConfig) -> Optional[str]:
    """Gets an Azure AD access token using service principal credentials."""
    if not all(
        [auth_cfg.client_id, auth_cfg.tenant_id, auth_cfg.client_secret]
    ):
        logger.error(
            "Missing client_id, tenant_id, or client_secret for SP auth."
        )
        return None
    try:
        authority = f"https://login.microsoftonline.com/{auth_cfg.tenant_id}"
        app = msal.ConfidentialClientApplication(
            auth_cfg.client_id,
            authority=authority,
            client_credential=auth_cfg.client_secret,
        )
        logger.debug(f"Attempting SP token acquisition for {auth_cfg.client_id}")
        result = app.acquire_token_for_client(
            scopes=["https://database.windows.net/.default"]
        )
        if "access_token" in result:
            logger.info("Successfully acquired token using service principal.")
            return result["access_token"]
        else:
            error_desc = result.get("error_description", "No description")
            logger.error(
                f"Failed SP token: {result.get('error', 'Unknown')}. {error_desc}"
            )
            return None
    except Exception as e:
        logger.exception(f"Exception during SP auth: {e}", exc_info=True)
        return None


def get_interactive_token(auth_cfg: AuthConfig) -> Optional[str]:
    """Gets an Azure AD access token using interactive browser login."""
    if not all([auth_cfg.client_id, auth_cfg.tenant_id]):
        logger.error("Missing client_id or tenant_id for interactive auth.")
        return None
    try:
        authority = f"https://login.microsoftonline.com/{auth_cfg.tenant_id}"
        app = msal.PublicClientApplication(
            auth_cfg.client_id, authority=authority
        )
        accounts = app.get_accounts(username=auth_cfg.username)
        if accounts:
            logger.info(
                f"Attempting silent token acquisition for: {accounts[0]['username']}"
            )
            result = app.acquire_token_silent(
                ["https://database.windows.net/.default"], account=accounts[0]
            )
            if result and "access_token" in result:
                logger.info("Successfully acquired token silently.")
                return result["access_token"]
            else:
                logger.info("Silent token acquisition failed or token expired.")

        logger.info("Starting interactive browser authentication...")
        result = app.acquire_token_interactive(
            scopes=["https://database.windows.net/.default"],
            prompt="select_account",
        )
        if "access_token" in result:
            logger.info("Successfully acquired token interactively.")
            return result["access_token"]
        else:
            error_desc = result.get("error_description", "No description")
            logger.error(
                f"Failed interactive token: {result.get('error', 'Unknown')}. {error_desc}"
            )
            return None
    except Exception as e:
        logger.exception(f"Exception during interactive auth: {e}", exc_info=True)
        return None


@contextmanager
def db_connection(
    server: str, database: str, auth_cfg: AuthConfig
) -> pyodbc.Connection:
    """Context manager for pyodbc connection with various auth methods."""
    base = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    conn: Optional[pyodbc.Connection] = None
    logger.info(
        f"Attempting connection to {server}/{database} using '{auth_cfg.method}'."
    )

    try:
        if auth_cfg.method == "service_principal":
            if not all(
                [
                    auth_cfg.client_id,
                    auth_cfg.tenant_id,
                    auth_cfg.client_secret,
                ]
            ):
                raise ValueError(
                    "SP auth requires client_id, tenant_id, client_secret."
                )
            conn_str = (
                base
                + f"Authentication=ActiveDirectoryServicePrincipal;UID={auth_cfg.client_id};PWD={auth_cfg.client_secret};"
            )
            conn = pyodbc.connect(conn_str, autocommit=True)
        elif auth_cfg.method == "interactive":
            token = get_interactive_token(auth_cfg)
            if not token:
                raise ConnectionError("Failed to obtain interactive Azure AD token.")
            token_bytes = bytes(token, "utf-16-le")
            token_struct = struct.pack(
                f"<i{len(token_bytes)}s", len(token_bytes), token_bytes
            )
            conn_str = base + "Authentication=ActiveDirectoryAccessToken;"
            conn = pyodbc.connect(
                conn_str, attrs_before={1256: token_struct}, autocommit=True
            )
        elif auth_cfg.method == "sql":
            if not all([auth_cfg.username, auth_cfg.password is not None]):
                raise ValueError("SQL auth requires username and password.")
            conn_str = base + f"UID={auth_cfg.username};PWD={auth_cfg.password};"
            conn = pyodbc.connect(conn_str, autocommit=True)
        else:  # 'windows' or default
            conn_str = base + "Trusted_Connection=yes;"
            conn = pyodbc.connect(conn_str, autocommit=True)

        logger.info(f"Successfully connected to {server}/{database}.")
        yield conn

    except pyodbc.Error as e:
        sqlstate = e.args[0]
        logger.error(
            f"pyodbc Error connecting (SQLSTATE: {sqlstate}): {e}"
        )
        if "HYT00" in str(e):
            logger.error("Connection timed out.")
        elif "28000" in str(e):
            logger.error("Authentication failed.")
        elif "08001" in str(e):
            logger.error("Cannot connect to server.")
        raise ConnectionError(f"Database connection failed: {e}") from e
    except Exception as e:
        logger.exception(f"Unexpected connection error: {e}", exc_info=True)
        raise ConnectionError(
            f"Unexpected connection failure: {e}"
        ) from e
    finally:
        if conn:
            try:
                conn.close()
                logger.debug(f"Closed connection to {server}/{database}.")
            except pyodbc.Error as e:
                logger.warning(f"Error closing connection: {e}")


# ========== FILE AND FOLDER UTILITIES ==========
def ensure_folders(base_path: Path) -> Dict[str, Path]:
    """Creates required subfolder structure."""
    paths = {}
    logger.debug(f"Ensuring standard folders under: {base_path}")
    try:
        if not base_path.exists():
            logger.warning(f"Base path {base_path} not found. Creating.")
            base_path.mkdir(parents=True, exist_ok=True)
        for name in SUBFOLDERS:
            folder_path = base_path / name
            folder_path.mkdir(exist_ok=True, parents=True)
            paths[name] = folder_path
            logger.debug(f"Ensured folder: {folder_path}")
        return paths
    except OSError as e:
        logger.error(f"Failed to create directories under {base_path}: {e}")
        raise


def _generate_versioned_filename(db_name: str, version: int) -> str:
    """Generates filename: YYYY-MM-DD_DatabaseName_vXXX.csv"""
    today_str = datetime.today().strftime("%Y-%m-%d")
    return f"{today_str}_{db_name}_v{str(version).zfill(VERSION_PADDING)}.csv"


def archive_file(src_path: Path, archive_folder: Path) -> bool:
    """Moves source file to archive folder, handling potential name collisions."""
    if not src_path.exists():
        logger.warning(f"Cannot archive missing file: {src_path}")
        return False

    archive_dest = archive_folder / src_path.name
    counter = 1
    while archive_dest.exists():
        archive_dest = archive_folder / f"{src_path.stem}_{counter}{src_path.suffix}"
        counter += 1
        if counter > 10:
            logger.error(
                f"Could not find unique archive name for {src_path.name}."
            )
            return False

    logger.debug(f"Archiving {src_path.name} to {archive_dest}")
    try:
        archive_folder.mkdir(exist_ok=True, parents=True)
        shutil.move(str(src_path), str(archive_dest))
        logger.info(f"Archived: {src_path.name} to {archive_dest.name}")
        return True
    except (OSError, shutil.Error) as e:
        logger.error(f"Failed archive {src_path.name}: {e}. File locked?")
        return False
    except Exception as e:
        logger.exception(
            f"Unexpected archive error for {src_path.name}: {e}", exc_info=True
        )
        return False


def cleanup_archives(archive_folder: Path, db_name: str, keep_count: int):
    """Removes old archived files for a specific db_name."""
    if keep_count < 0:
        logger.warning(f"Archive keep_count negative. Setting to 0.")
        keep_count = 0
    if keep_count == 0:
        logger.warning(
            f"Archive keep_count is 0 for {db_name}. ALL archives will be deleted."
        )

    logger.debug(
        f"Cleaning archives in {archive_folder} for '{db_name}', keeping {keep_count}"
    )
    try:
        pattern = f"*_{db_name}_v*.csv"
        files_with_versions = []
        for f in archive_folder.glob(pattern):
            if f.is_file():
                try:
                    version = int(f.stem.split("_v")[-1])
                    files_with_versions.append((f, f.stat().st_mtime, version))
                except (IndexError, ValueError):
                    logger.warning(f"Could not parse version from archive: {f.name}.")
                    continue

        # Sort descending by version, then modification time
        files_with_versions.sort(key=lambda x: (x[2], x[1]), reverse=True)
        files_to_delete = [fwv[0] for fwv in files_with_versions[keep_count:]]

        deleted_count = 0
        for outdated_file in files_to_delete:
            try:
                outdated_file.unlink()
                logger.info(f"Deleted old archive: {outdated_file.name}")
                deleted_count += 1
            except OSError as e:
                logger.warning(f"Failed delete old archive {outdated_file.name}: {e}")
            except Exception as e:
                logger.exception(
                    f"Unexpected error deleting archive {outdated_file.name}: {e}",
                    exc_info=True,
                )

        if deleted_count > 0:
            logger.info(f"Deleted {deleted_count} old archive(s) for {db_name}.")
        elif len(files_to_delete) > 0:
            logger.warning(
                f"Attempted delete {len(files_to_delete)} archive(s) for {db_name}, none successful."
            )
        else:
            logger.debug(f"No old archives found to delete for {db_name}.")

    except Exception as e:
        logger.exception(
            f"Error during archive cleanup for {db_name}: {e}", exc_info=True
        )


# ========== VERSIONING & LOCKING ==========
@contextmanager
def acquire_version_lock(lock_file_path: Path, timeout: int = 30):
    """Acquires an exclusive lock on the version file using a lock file."""
    lock_acquired = False
    start_time = time.time()
    file_handle = None
    try:
        logger.debug(f"Attempting lock: {lock_file_path} (Method: {LOCK_METHOD})")
        while time.time() - start_time < timeout:
            try:
                if LOCK_METHOD == "msvcrt":
                    file_handle = os.open(
                        lock_file_path, os.O_CREAT | os.O_RDWR | os.O_EXCL
                    )
                    lock_acquired = True
                    break
                elif LOCK_METHOD == "fcntl":
                    file_handle = os.open(lock_file_path, os.O_CREAT | os.O_RDWR)
                    fcntl.flock(file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    lock_acquired = True
                    break
                else:  # 'file' method
                    file_handle = os.open(
                        lock_file_path, os.O_CREAT | os.O_WRONLY | os.O_EXCL
                    )
                    os.close(file_handle)  # Close immediately
                    file_handle = None
                    lock_acquired = True
                    break
            except (IOError, OSError) as e:
                if file_handle is not None:
                    try:
                        os.close(file_handle)
                        file_handle = None
                    except OSError:
                        pass
                logger.debug(
                    f"Lock busy/error acquiring ({e.__class__.__name__}), retrying..."
                )
                time.sleep(0.5 + (time.time() % 0.5))
            except Exception as e:
                logger.error(f"Unexpected lock acquisition error: {e}")
                if file_handle is not None:
                    try:
                        os.close(file_handle)
                    except OSError:
                        pass
                raise

        if not lock_acquired:
            raise TimeoutError(
                f"Could not acquire lock on {lock_file_path.name} within {timeout}s."
            )

        logger.debug(f"Lock acquired: {lock_file_path}")
        yield  # Lock is held

    finally:
        if lock_acquired:
            try:
                if file_handle is not None:
                    if LOCK_METHOD == "fcntl":
                        fcntl.flock(file_handle, fcntl.LOCK_UN)
                    os.close(file_handle)
                    file_handle = None
                # Always try to remove the lock file after releasing handle/lock
                lock_file_path.unlink(missing_ok=True)
                logger.debug(f"Lock released: {lock_file_path}")
            except (OSError, IOError) as e:
                logger.error(
                    f"Error releasing lock/removing file {lock_file_path}: {e}"
                )
            except Exception as e:
                logger.exception(
                    f"Unexpected lock release error {lock_file_path}: {e}",
                    exc_info=True,
                )


def load_versions(version_file_path: Path) -> Dict[str, int]:
    """Loads version information from the JSON version file with locking."""
    lock_file_path = version_file_path.with_suffix(VERSION_LOCK_FILE_NAME_SUFFIX)
    version_data = {}
    try:
        with acquire_version_lock(lock_file_path):
            if not version_file_path.exists():
                logger.info(f"Version file not found: {version_file_path}. Starting empty.")
                return {}
            try:
                with open(version_file_path, "r", encoding=ENCODING_UTF8_SIG) as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    logger.warning(
                        f"Invalid version file format: {version_file_path}. Resetting."
                    )
                    return {}
                version_data = {
                    k: v
                    for k, v in data.items()
                    if isinstance(k, str) and isinstance(v, int)
                }
                if len(version_data) != len(data):
                    logger.warning(
                        f"Version file {version_file_path.name} had invalid entries."
                    )
                logger.info(
                    f"Loaded {len(version_data)} versions from {version_file_path.name}"
                )
                return version_data
            except json.JSONDecodeError:
                logger.error(
                    f"Invalid JSON in version file {version_file_path}. Trying backup."
                )
                backup_path = version_file_path.with_suffix(VERSION_BACKUP_FILE_NAME_SUFFIX)
                if backup_path.exists():
                    try:
                        logger.info(f"Restoring versions from backup: {backup_path.name}")
                        shutil.copy2(backup_path, version_file_path)
                        with open(
                            version_file_path, "r", encoding=ENCODING_UTF8_SIG
                        ) as f:
                            data = json.load(f)
                        if isinstance(data, dict):
                            version_data = {
                                k: v
                                for k, v in data.items()
                                if isinstance(k, str) and isinstance(v, int)
                            }
                            logger.info(
                                f"Restored/loaded {len(version_data)} versions."
                            )
                            return version_data
                        else:
                            logger.error("Backup invalid too. Resetting.")
                            return {}
                    except Exception as backup_e:
                        logger.error(
                            f"Failed load/restore backup {backup_path.name}: {backup_e}. Resetting."
                        )
                        return {}
                else:
                    logger.error("No backup found. Resetting.")
                    return {}
            except Exception as e:
                logger.error(
                    f"Error loading version file {version_file_path}: {e}. Resetting."
                )
                return {}
    except (TimeoutError, OSError, IOError) as lock_e:
        logger.error(
            f"Failed lock for loading version file {version_file_path}: {lock_e}"
        )
        raise RuntimeError(f"Could not load versions: {lock_e}") from lock_e
    except Exception as e:
        logger.exception(f"Unexpected version loading error: {e}", exc_info=True)
        raise


def save_versions(version_file_path: Path, data: Dict[str, int]):
    """Saves version information to JSON file with locking and backup."""
    lock_file_path = version_file_path.with_suffix(VERSION_LOCK_FILE_NAME_SUFFIX)
    backup_path = version_file_path.with_suffix(VERSION_BACKUP_FILE_NAME_SUFFIX)

    logger.debug(f"Attempting save {len(data)} versions to {version_file_path.name}")
    try:
        with acquire_version_lock(lock_file_path):
            # 1. Create backup
            if version_file_path.exists():
                try:
                    shutil.copy2(version_file_path, backup_path)
                    logger.debug(f"Backup created: {backup_path.name}")
                except Exception as backup_e:
                    logger.warning(
                        f"Could not backup {version_file_path.name}: {backup_e}"
                    )
            # 2. Write new data
            try:
                version_file_path.parent.mkdir(exist_ok=True)
                with open(version_file_path, "w", encoding=ENCODING_UTF8_SIG) as f:
                    json.dump(data, f, indent=2, sort_keys=True)
                logger.info(f"Saved {len(data)} versions to {version_file_path.name}")
            except Exception as e:
                logger.error(f"Error saving version file {version_file_path}: {e}")
                # Attempt restore on failure
                if backup_path.exists():
                    try:
                        shutil.copy2(backup_path, version_file_path)
                        logger.info("Restored from backup.")
                    except Exception as restore_e:
                        logger.error(f"CRITICAL: Save AND restore failed: {restore_e}")
                else:
                    logger.error("CRITICAL: Save failed, no backup exists.")
                raise  # Re-raise original error after attempting recovery

    except (TimeoutError, OSError, IOError) as lock_e:
        logger.error(f"Failed lock for saving {version_file_path}: {lock_e}")
        raise RuntimeError(f"Could not save versions: {lock_e}") from lock_e
    except Exception as e:
        logger.exception(f"Unexpected version saving error: {e}", exc_info=True)
        raise


# ========== SCHEMA FETCHING AND PROCESSING ==========
def fetch_schema(
    server: str, db: str, user_input_cfg: UserInputConfig, auth_cfg: AuthConfig
) -> Optional[pd.DataFrame]:
    """Fetches schema using SQL_QUERY. No parameters needed now."""
    params = None
    logger.info(f"Fetching schema for database: {db} on server: {server}")
    try:
        with db_connection(server, db, auth_cfg) as conn:
            df = pd.read_sql(SQL_QUERY, conn, params=params)
            logger.info(f"Successfully fetched {len(df)} schema rows for {db}.")
            if df.empty:
                logger.warning(f"Schema query returned no results for {db}.")
            elif not all(col in df.columns for col in KEY_COLS):
                logger.error(f"Schema missing keys: {KEY_COLS}")
                return None
            # Ensure key columns are strings
            for col in KEY_COLS:
                if col in df.columns:
                    df[col] = df[col].astype(str).fillna("")
            return df
    except ConnectionError as e:
        logger.error(f"Schema fetch connection error: {e}")
        return None
    except pd.errors.DatabaseError as e:
        logger.error(f"Pandas SQL Error fetching: {e}")
        if "timeout" in str(e).lower():
            logger.error("Query timeout.")
        return None
    except Exception as e:
        logger.exception(f"Unexpected schema fetch error: {e}", exc_info=True)
        return None


def merge_previous_metadata(
    current_df: pd.DataFrame, previous_file_path: Optional[Path]
) -> pd.DataFrame:
    """Merges current schema with previous version for PRESERVED_COLS."""
    if not previous_file_path or not previous_file_path.exists():
        logger.debug("No previous schema file found for merging.")
        for col in PRESERVED_COLS:  # Only ensure truly preserved cols exist
            if col not in current_df.columns:
                current_df[col] = pd.NA
        return current_df

    logger.info(f"Merging metadata from previous: {previous_file_path.name}")
    try:
        previous_df = pd.read_csv(
            previous_file_path, encoding=ENCODING_UTF8_SIG, dtype=str
        )
        # Ensure key columns are strings in previous df
        for col in KEY_COLS:
            if col in previous_df.columns:
                previous_df[col] = previous_df[col].astype(str).fillna("")

        # Check for missing columns in previous file
        required = KEY_COLS + PRESERVED_COLS
        missing = [c for c in required if c not in previous_df.columns]
        if missing:
            logger.warning(
                f"Previous file {previous_file_path.name} missing columns: {missing}."
            )
            for col in missing:
                if col in PRESERVED_COLS:
                    previous_df[col] = pd.NA  # Add missing preserved columns

        # Prepare previous data for merge
        previous_preserved_cols = KEY_COLS + [
            c for c in PRESERVED_COLS if c in previous_df.columns
        ]
        previous_preserved = previous_df[previous_preserved_cols].copy()
        previous_preserved.dropna(subset=KEY_COLS, how="all", inplace=True)

        # Ensure key columns are strings in current df
        for col in KEY_COLS:
            if col in current_df.columns:
                current_df[col] = current_df[col].astype(str).fillna("")
            else:
                # This should not happen if fetch_schema validation works
                logger.error(f"CRITICAL: Key '{col}' missing from current fetch.")
                return current_df # Return unmodified on critical error

        # Perform merge
        merged_df = current_df.merge(
            previous_preserved, on=KEY_COLS, how="left", suffixes=("", "_prev")
        )

        # Apply previous values to preserved columns where current is null
        for col in PRESERVED_COLS:  # Only iterate over truly preserved cols
            prev_col = f"{col}_prev"
            if prev_col in merged_df.columns:
                if col in merged_df.columns:
                    # combine_first fills NaN in `col` with values from `prev_col`
                    merged_df[col] = merged_df[col].combine_first(merged_df[prev_col])
                else:
                    # If col didn't exist in current, take the previous one
                    merged_df[col] = merged_df[prev_col]
                merged_df.drop(columns=[prev_col], inplace=True)
            elif col not in merged_df.columns:
                # Ensure column exists even if not in current or previous
                merged_df[col] = pd.NA

        logger.info(f"Successfully merged metadata from {previous_file_path.name}.")
        return merged_df

    except pd.errors.EmptyDataError:
        logger.warning(f"Previous schema file {previous_file_path.name} empty.")
    except Exception as e:
        logger.exception(
            f"Error merging {previous_file_path.name}: {e}", exc_info=True
        )

    # Ensure preserved columns exist even on error/empty previous
    for col in PRESERVED_COLS:
        if col not in current_df.columns:
            current_df[col] = pd.NA
    return current_df


def find_latest_schema_file(output_folder: Path, db_name: str) -> Optional[Path]:
    """Finds the latest versioned schema file for a given database name."""
    latest_file = None
    latest_version = -1
    files_found = 0
    pattern = f"*_{db_name}_v*.csv"

    for f in output_folder.glob(pattern):
        if f.is_file():
            files_found += 1
            try:
                version = int(f.stem.split("_v")[-1])
                if version > latest_version:
                    latest_version = version
                    latest_file = f
                elif (
                    version == latest_version
                    and latest_file
                    and f.stat().st_mtime > latest_file.stat().st_mtime
                ):
                    # Tie-break with modification time if versions are equal
                    latest_file = f
            except (IndexError, ValueError):
                logger.warning(f"Could not parse version from file: {f.name}.")
                continue

    if latest_file:
        logger.debug(
            f"Found latest schema file for {db_name}: {latest_file.name} (V {latest_version})"
        )
    elif files_found > 0:
        logger.warning(
            f"Found {files_found} file(s) for {db_name} but couldn't determine latest reliably."
        )
    else:
        logger.debug(f"No previous schema file found for {db_name} in {output_folder}.")
    return latest_file


# ========== CORE PROCESSING LOGIC ==========
def export_schema_for_db(
    domain_cfg: DomainConfig,
    db_name: str,
    paths: Dict[str, Path],
    version_data: Dict[str, int],
    user_input_cfg: UserInputConfig,
    auth_cfg: AuthConfig,
) -> Union[Path, bool, None]:
    """
    Exports schema, returns Path on success, True if skipped, False/None on failure.
    """
    logger.info(f"--- Starting export process for database: {db_name} ---")
    version_base_key = db_name
    sql_output_folder = paths["002 SQL Output"]
    archive_folder = paths["001 Archive"]
    version_file_path = sql_output_folder / VERSION_FILE_NAME

    # Determine current version, checking file system as fallback
    previous_filepath = find_latest_schema_file(sql_output_folder, db_name)
    current_version = version_data.get(version_base_key, 0) # Default to 0
    if previous_filepath:
        try:
            version_from_file = int(previous_filepath.stem.split("_v")[-1])
            if version_from_file > current_version:
                logger.warning(
                    f"Version mismatch ({version_from_file} file vs {current_version} json). Using file."
                )
                current_version = version_from_file
        except (IndexError, ValueError):
            logger.warning(f"Could not parse version from {previous_filepath.name}")

    # Define paths for next version
    next_version = current_version + 1
    next_filename = _generate_versioned_filename(db_name, next_version)
    next_filepath = sql_output_folder / next_filename
    temp_filepath = sql_output_folder / (next_filename + ".tmp")

    # 1. Fetch schema
    current_schema_df = fetch_schema(
        domain_cfg.sql_server, db_name, user_input_cfg, auth_cfg
    )
    if current_schema_df is None:
        logger.error(f"Schema fetch failed for {db_name}.")
        return False
    if current_schema_df.empty:
        logger.warning(f"Schema for {db_name} is empty.")

    # 2. Merge metadata
    merged_df = merge_previous_metadata(current_schema_df, previous_filepath)

    # 3. Check for changes
    if previous_filepath and previous_filepath.exists():
        try:
            previous_df = pd.read_csv(
                previous_filepath, encoding=ENCODING_UTF8_SIG, dtype=str
            )
            # Ensure key columns are strings for comparison
            for col in KEY_COLS:
                if col in merged_df.columns:
                    merged_df[col] = merged_df[col].astype(str).fillna("")
                if col in previous_df.columns:
                    previous_df[col] = previous_df[col].astype(str).fillna("")

            if merged_df.equals(previous_df):
                logger.info(f"No changes vs {previous_filepath.name}. Skipping.")
                return True # Indicate skipped
            else:
                logger.info(f"Schema content has changed for {db_name}.")
        except Exception as e:
            logger.warning(
                f"Could not compare with {previous_filepath.name}: {e}. Assuming changes."
            )
    else:
        logger.info(f"No previous file for {db_name}. Creating initial.")

    # 4. Write to temp file
    try:
        logger.debug(f"Writing schema to temp: {temp_filepath}")
        merged_df.to_csv(temp_filepath, index=False, encoding=ENCODING_UTF8_SIG)
    except Exception as e:
        logger.exception(f"Failed write temp {temp_filepath}: {e}")
        if temp_filepath.exists():
            temp_filepath.unlink(missing_ok=True)
        return False

    # 5. Archive previous version
    if previous_filepath and previous_filepath.exists():
        logger.info(f"Archiving previous: {previous_filepath.name}")
        if archive_file(previous_filepath, archive_folder):
            cleanup_archives(
                archive_folder, db_name, user_input_cfg.archive_keep_count
            )
        else:
            logger.error(f"Failed archive {previous_filepath.name}. Aborting.")
            if temp_filepath.exists():
                temp_filepath.unlink(missing_ok=True)
            return False

    # 6. Rename temp file to final destination
    try:
        logger.debug(f"Renaming {temp_filepath.name} -> {next_filepath.name}")
        shutil.move(str(temp_filepath), str(next_filepath))
        logger.info(f"Successfully exported schema: {next_filepath.name} (V{next_version})")

        # 7. Update version data
        version_data[version_base_key] = next_version
        save_versions(version_file_path, version_data)

        logger.info(f"--- Finished export for database: {db_name} ---")
        return next_filepath  # Return Path on success

    except (OSError, shutil.Error) as e:
        logger.error(
            f"Failed rename {temp_filepath.name} -> {next_filepath.name}: {e}"
        )
        if temp_filepath.exists():
            temp_filepath.unlink(missing_ok=True)
        logger.critical(
            f"Inconsistent state for {db_name}: Previous archived, new failed."
        )
        return False
    except Exception as e:
        logger.exception(
            f"Unexpected final export error {db_name}: {e}", exc_info=True
        )
        if temp_filepath.exists():
            temp_filepath.unlink(missing_ok=True)
        return False


# ========== USER INPUT HANDLING ==========
def check_user_input_for_updates(
    sql_output_folder: Path, user_input_folder: Path
) -> List[Tuple[Path, Optional[Path]]]:
    """Checks the User Input folder for files."""
    potential_updates = []
    logger.info(f"Checking user input in: {user_input_folder}")
    if not user_input_folder.exists():
        logger.warning(f"User input folder not found: {user_input_folder}")
        return []

    try:
        for user_file_path in user_input_folder.glob("*.csv"):
            if not user_file_path.is_file():
                continue

            logger.debug(f"Found potential user input file: {user_file_path.name}")
            db_name = None
            try:
                # Heuristic DB name extraction (adjust if needed)
                parts = user_file_path.stem.split("_v")[0].split("_")
                if len(parts) > 1 and parts[0].count("-") == 2:
                    db_name = "_".join(parts[1:])
                else:
                    db_name = user_file_path.stem.split("_v")[0]
            except Exception:
                logger.warning(
                    f"Could not determine DB name from {user_file_path.name}. Skipping."
                )
                continue
            if not db_name:
                logger.warning(
                    f"Could not extract DB name for {user_file_path.name}. Skipping."
                )
                continue

            latest_sql_file = find_latest_schema_file(sql_output_folder, db_name)
            log_msg = f"User input '{user_file_path.name}' (DB '{db_name}')."
            if latest_sql_file:
                log_msg += f" Found SQL output: '{latest_sql_file.name}'."
            else:
                log_msg += " No existing SQL output."
            logger.info(log_msg)
            potential_updates.append((user_file_path, latest_sql_file))

    except Exception as e:
        logger.exception(
            f"Error checking user input folder {user_input_folder}: {e}",
            exc_info=True,
        )

    logger.info(f"Found {len(potential_updates)} potential user input files.")
    return potential_updates


def process_user_updates(
    updates: List[Tuple[Path, Optional[Path]]],
    version_data: Dict[str, int],
    paths: Dict[str, Path],
    user_input_cfg: UserInputConfig,
) -> List[Dict[str, Any]]:
    """
    Processes user input files, returns list of {'fileName':..., 'filePath':...}.
    """
    if not updates:
        logger.info("No user updates to process.")
        return []

    sql_output_folder = paths["002 SQL Output"]
    archive_folder = paths["001 Archive"]
    version_file_path = sql_output_folder / VERSION_FILE_NAME

    processed_files_info = []
    processed_count = 0
    failed_count = 0

    for user_file_path, sql_to_replace_path_opt in updates:
        user_file_name = user_file_path.name
        db_name = None
        try:
            # Re-extract db_name
            parts = user_file_path.stem.split("_v")[0].split("_")
            if len(parts) > 1 and parts[0].count("-") == 2:
                db_name = "_".join(parts[1:])
            else:
                db_name = user_file_path.stem.split("_v")[0]
            if not db_name:
                raise ValueError("Could not extract DB name")

            version_base_key = db_name
            logger.info(f"Processing user input '{user_file_name}' for DB '{db_name}'")

            # --- Basic User File Validation ---
            try:
                df_user = pd.read_csv(
                    user_file_path, encoding=ENCODING_UTF8_SIG
                )
                if df_user.empty:
                    logger.warning(f"User file '{user_file_name}' empty. Skipping.")
                    continue
                missing = [c for c in KEY_COLS if c not in df_user.columns]
                if missing:
                    logger.error(
                        f"User file '{user_file_name}' missing keys: {missing}. Skipping."
                    )
                    failed_count += 1
                    continue
                for col in KEY_COLS:
                    df_user[col] = df_user[col].astype(str).fillna("")
                logger.debug(f"User file '{user_file_name}' passed validation.")
            except Exception as read_err:
                logger.error(
                    f"Error read/validate user file '{user_file_name}': {read_err}. Skipping."
                )
                failed_count += 1
                continue
            # --- End Validation ---

            # Determine version number for new file
            current_version = version_data.get(version_base_key, 0)
            if sql_to_replace_path_opt:
                try:
                    v_file = int(sql_to_replace_path_opt.stem.split("_v")[-1])
                    if v_file > current_version:
                        current_version = v_file
                except (IndexError, ValueError):
                    pass  # Ignore if parsing fails
            next_version = current_version + 1

            # Define paths
            new_sql_output_filename = _generate_versioned_filename(
                db_name, next_version
            )
            new_sql_output_path = sql_output_folder / new_sql_output_filename
            temp_filepath = sql_output_folder / (new_sql_output_filename + ".tmp")

            # Write user data to temp
            try:
                logger.debug(f"Writing user data to temp: {temp_filepath}")
                df_user.to_csv(
                    temp_filepath, index=False, encoding=ENCODING_UTF8_SIG
                )
            except Exception as write_err:
                logger.error(
                    f"Failed write user temp {temp_filepath}: {write_err}. Skip {user_file_name}."
                )
                if temp_filepath.exists():
                    temp_filepath.unlink(missing_ok=True)
                failed_count += 1
                continue

            # Archive SQL file being replaced
            if sql_to_replace_path_opt and sql_to_replace_path_opt.exists():
                logger.info(f"Archiving existing: '{sql_to_replace_path_opt.name}'")
                if not archive_file(sql_to_replace_path_opt, archive_folder):
                    logger.error(
                        f"Failed archive '{sql_to_replace_path_opt.name}'. Aborting {user_file_name}."
                    )
                    if temp_filepath.exists():
                        temp_filepath.unlink(missing_ok=True)
                    failed_count += 1
                    continue
                else:
                    cleanup_archives(
                        archive_folder, db_name, user_input_cfg.archive_keep_count
                    )

            # Rename temp to final new versioned file
            try:
                logger.debug(f"Renaming {temp_filepath.name} -> {new_sql_output_path.name}")
                shutil.move(str(temp_filepath), str(new_sql_output_path))
                logger.info(
                    f"Applied user input: Created '{new_sql_output_filename}' (V{next_version}) from '{user_file_name}'."
                )
                version_data[version_base_key] = next_version
                save_versions(version_file_path, version_data)
                # Add info about the created file to the list to be returned
                processed_files_info.append(
                    {
                        "fileName": new_sql_output_filename,
                        "filePath": new_sql_output_path, # Store Path object
                    }
                )
                # Remove processed user input file
                try:
                    logger.debug(f"Removing user file: {user_file_name}")
                    user_file_path.unlink()
                    logger.info(f"Removed user file: {user_file_name}")
                except OSError as e:
                    logger.warning(f"Could not remove user file {user_file_name}: {e}.")
                processed_count += 1
            except (OSError, shutil.Error) as move_err:
                logger.error(
                    f"Failed rename {temp_filepath.name} -> {new_sql_output_path.name}: {move_err}"
                )
                if temp_filepath.exists():
                    temp_filepath.unlink(missing_ok=True)
                logger.critical(
                    f"Inconsistent state for {db_name} due to user update failure."
                )
                failed_count += 1
            except Exception as final_err:
                logger.exception(
                    f"Unexpected error applying user update {user_file_name}: {final_err}",
                    exc_info=True,
                )
                if temp_filepath.exists():
                    temp_filepath.unlink(missing_ok=True)
                failed_count += 1
        except Exception as outer_err:
            logger.exception(
                f"Failed process user update {user_file_name}: {outer_err}",
                exc_info=True,
            )
            failed_count += 1

    logger.info(f"Finished user updates: {processed_count} succeeded, {failed_count} failed.")
    return processed_files_info


# ========== DOMAIN PROCESSING LOGIC ==========
def get_databases_for_domain(
    domain_cfg: DomainConfig, auth_cfg: AuthConfig
) -> List[str]:
    """Gets list of database names to process for a domain."""
    if domain_cfg.db_override:
        logger.info(
            f"Using override DB list for '{domain_cfg.name}': {', '.join(domain_cfg.db_override)}"
        )
        return domain_cfg.db_override
    if domain_cfg.db_prefix:
        if "fabric.microsoft.com" in domain_cfg.sql_server.lower():
            logger.warning(
                f"Domain '{domain_cfg.name}': Fabric + db_prefix unreliable. Use 'db_override'."
            )
        logger.info(
            f"Querying DBs on {domain_cfg.sql_server} prefix '{domain_cfg.db_prefix}' for '{domain_cfg.name}'."
        )
        databases = []
        try:
            # Attempt connection to master, may fail on Fabric
            with db_connection(domain_cfg.sql_server, "master", auth_cfg) as conn:
                safe_prefix = domain_cfg.db_prefix.replace("'", "''")
                query = f"SELECT name FROM sys.databases WHERE name LIKE '{safe_prefix}%' ORDER BY name"
                cursor = conn.cursor()
                cursor.execute(query)
                databases = [row.name for row in cursor.fetchall()]
                if not databases:
                    logger.warning(
                        f"No DBs on {domain_cfg.sql_server} match prefix '{domain_cfg.db_prefix}'."
                    )
                else:
                    logger.info(
                        f"Found {len(databases)} DBs for '{domain_cfg.name}': {', '.join(databases)}"
                    )
                return databases
        except ConnectionError as e:
            logger.error(
                f"Failed connect {domain_cfg.sql_server}/master for DB list: {e}"
            )
            if "fabric.microsoft.com" in domain_cfg.sql_server.lower():
                logger.error("Expected for Fabric. Use 'db_override'.")
            return []
        except pyodbc.Error as e:
            sqlstate = e.args[0]
            logger.error(
                f"SQL error query DBs {domain_cfg.sql_server} (SQLSTATE: {sqlstate}): {e}"
            )
            if "42S02" in str(sqlstate):
                logger.error("Could not find 'sys.databases'. Check permissions/context.")
            return []
        except Exception as e:
            logger.exception(
                f"Failed retrieve DB list '{domain_cfg.db_prefix}': {e}",
                exc_info=True,
            )
            return []

    logger.warning(
        f"No 'db_override' or 'db_prefix' for '{domain_cfg.name}'. Cannot determine databases."
    )
    return []


def construct_sharepoint_link(
    file_path: Path, domain_cfg: DomainConfig
) -> Optional[str]:
    """
    Attempts basic SharePoint link construction.
    REQUIRES CONFIGURATION & REVIEW.
    """
    if (
        not domain_cfg.sharepoint_site_url
        or not domain_cfg.sharepoint_doc_library_path
    ):
        logger.debug(
            f"SP link skipped: sharepoint_site_url/sharepoint_doc_library_path not configured for '{domain_cfg.name}'."
        )
        return None
    try:
        # Assumes sharepoint_path in config is the local root sync folder
        relative_file_path = file_path.relative_to(domain_cfg.sharepoint_path)
        # URL Encode the relative path using POSIX separators
        encoded_relative_path = urllib.parse.quote(relative_file_path.as_posix())
        # Combine parts, ensuring no double slashes
        base_url = domain_cfg.sharepoint_site_url.rstrip("/")
        # Assumes library path is already URL-encoded if needed (e.g., Shared%20Documents)
        library_path = domain_cfg.sharepoint_doc_library_path.strip("/")
        full_link = f"{base_url}/{library_path}/{encoded_relative_path}"
        logger.debug(f"Constructed SP link: {full_link}")
        return full_link
    except ValueError:
        # Error if file_path is not inside domain_cfg.sharepoint_path
        logger.error(
            f"Could not determine relative path for link: '{file_path}' not inside '{domain_cfg.sharepoint_path}'."
        )
        return None
    except Exception as e:
        logger.exception(
            f"Error constructing SP link for {file_path}: {e}", exc_info=True
        )
        return None


def process_domain(
    domain_cfg: DomainConfig,
    user_input_cfg: UserInputConfig,
    auth_cfg: AuthConfig,
    flow_urls: Dict[str, str],
):
    """
    Processes a domain: exports schemas, handles user input, sends ONE summary notification.
    """
    logger.info(f"===== Processing Domain: {domain_cfg.name} =====")
    logger.info(f"SP Path: {domain_cfg.sharepoint_path}")
    logger.info(f"SQL Server: {domain_cfg.sql_server}")
    logger.info(f"Archive Keep: {user_input_cfg.archive_keep_count}")

    try:
        paths = ensure_folders(domain_cfg.sharepoint_path)
    except Exception as e:
        logger.error(f"Cannot proceed: Dir prep failed: {e}")
        return

    sql_output_folder = paths["002 SQL Output"]
    user_input_folder = paths["003 User Input"]
    version_file_path = sql_output_folder / VERSION_FILE_NAME

    try:
        version_data = load_versions(version_file_path)
    except (RuntimeError, Exception) as e:
        logger.error(f"Failed load version data: {e}. Skip domain.")
        return

    # --- Initialize tracking lists and counters ---
    modified_files_for_notification = []
    successful_exports = 0
    skipped_exports = 0
    failed_exports = 0
    exported_dbs = set()

    # --- Process Database Exports ---
    databases_to_process = get_databases_for_domain(domain_cfg, auth_cfg)
    if not databases_to_process:
        logger.warning(f"No DBs identified for '{domain_cfg.name}'. Checking user inputs only.")
    else:
        for db_name in databases_to_process:
            if db_name in exported_dbs:
                logger.debug(f"DB {db_name} already processed. Skipping.")
                continue
            try:
                export_result = export_schema_for_db(
                    domain_cfg,
                    db_name,
                    paths,
                    version_data,
                    user_input_cfg,
                    auth_cfg,
                )

                if isinstance(export_result, Path): # Success, file created/updated
                    new_file_path = export_result
                    logger.info(f"DB {db_name} OK. File: {new_file_path.name}")
                    sp_link = construct_sharepoint_link(new_file_path, domain_cfg)
                    modified_files_for_notification.append(
                        {
                            "fileName": new_file_path.name,
                            "sharepointLink": sp_link or "", # Send empty string if link fails
                        }
                    )
                    successful_exports += 1
                elif export_result is True: # Skipped, no changes
                    skipped_exports += 1
                    logger.debug(f"DB {db_name} export skipped (no changes).")
                else: # Failure
                    failed_exports += 1
                exported_dbs.add(db_name) # Mark as processed (even if failed)

            except Exception as e:
                logger.exception(
                    f"Unhandled export error for DB {db_name}: {e}", exc_info=True
                )
                failed_exports += 1
                exported_dbs.add(db_name) # Mark as processed

        logger.info(
            f"'{domain_cfg.name}' DB export summary: {successful_exports} succeeded, "
            f"{skipped_exports} skipped, {failed_exports} failed."
        )

    # --- Process User Updates ---
    try:
        user_updates = check_user_input_for_updates(
            sql_output_folder, user_input_folder
        )
        if user_updates:
            logger.info(
                f"Processing {len(user_updates)} user input file(s) for '{domain_cfg.name}'."
            )
            processed_user_files_info = process_user_updates(
                user_updates, version_data, paths, user_input_cfg
            )
            # Add successfully processed user files to notification list
            for file_info in processed_user_files_info:
                sp_link = construct_sharepoint_link(file_info["filePath"], domain_cfg)
                modified_files_for_notification.append(
                    {
                        "fileName": file_info["fileName"],
                        "sharepointLink": sp_link or "", # Send empty string if link fails
                    }
                )
        else:
            logger.info(f"No pending user input files for '{domain_cfg.name}'.")
    except Exception as e:
        logger.exception(
            f"Error processing user input for {domain_cfg.name}: {e}", exc_info=True
        )

    # --- Send ONE summary notification if files were modified ---
    if modified_files_for_notification:
        notification_url = flow_urls.get(domain_cfg.name) # Get URL using domain name as key
        if notification_url:
            send_summary_notification(
                domain_cfg.name,
                modified_files_for_notification,
                notification_url,
                user_input_cfg.created_by # Use configured user as trigger source
            )
        else:
            logger.warning(
                f"No notification_trigger_url configured for '{domain_cfg.name}'. Skipping summary notification."
            )
    else:
        logger.info(f"No files modified this run for '{domain_cfg.name}'. No notification sent.")

    logger.info(f"===== Finished Processing Domain: {domain_cfg.name} =====")


# ========== HTTP NOTIFICATION FUNCTION =========

# --- MODIFIED Function to Send the Notification ---

def send_summary_notification(
    domain_name: str,
    file_list: list,
    flow_url: str, # This URL now *includes* the SAS token (?sp=...&sv=...&sig=...)
    triggered_by: str,
    # Removed auth_cfg as it's not needed for SAS URL auth
):
    """
    Sends the summary payload to the Power Automate HTTP trigger URL.
    Assumes authentication is handled by SAS parameters within the flow_url.
    """
    if not flow_url:
        logger.warning(
            f"No Power Automate flow URL provided for domain '{domain_name}'. Skipping notification."
        )
        return

    # --- Token Acquisition and Auth Header REMOVED ---

    payload = {
        "domainName": domain_name,
        "modifiedFiles": file_list,
        "triggeredBy": triggered_by,
    }
    headers = {"Content-Type": "application/json"} # Only Content-Type needed

    logger.info(
        f"Sending summary notification for {len(file_list)} file(s) for '{domain_name}' to PA trigger."
    )
    logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
    # Ensure the full URL from config (including SAS token) is used
    logger.debug(f"Target URL (check for SAS params): {flow_url}")

    try:
        # Make the POST request using the full URL from config
        resp = requests.post(
            flow_url, headers=headers, json=payload, timeout=30
        )
        resp.raise_for_status() # Check for errors
        logger.info(
            f"✅ Summary notification successfully sent via PA for '{domain_name}'. Status: {resp.status_code}"
        )
    except requests.exceptions.Timeout:
        logger.error(f"❌ Timeout sending summary notification for '{domain_name}'.")
    except requests.exceptions.RequestException as exc:
        logger.error(
            f"❌ Failed send summary notification via PA for '{domain_name}': {exc}"
        )
        if hasattr(exc, "response") and exc.response is not None:
            logger.error(f"    Response status: {exc.response.status_code}")
            response_body = "(Could not get body)"
            try:
                response_body = (
                    exc.response.json()
                    if "json" in exc.response.headers.get("Content-Type", "")
                    else exc.response.text
                )
            except Exception:
                pass
            logger.error(f"    Response body: {response_body}")
            # Check if it's still an auth error (though SAS errors might manifest differently)
            if exc.response.status_code in [401, 403]:
                 logger.error("    Received Auth error. Check if the full Flow URL including ?sp=...&sv=...&sig=... was copied correctly to config.ini and that the SAS token is valid/not expired.")

    except Exception as e:
        logger.exception(
            f"❌ Unexpected PA notification error for '{domain_name}': {e}",
            exc_info=True,
        )


# ========== CONFIGURATION LOADING ==========
def load_config(
    config_file="config.ini",
) -> Tuple[List[DomainConfig], UserInputConfig, AuthConfig, Dict[str, str]]:
    """Loads configuration from the INI file."""
    config = configparser.ConfigParser(interpolation=None, allow_no_value=True)
    config_path = Path(config_file)
    if not config_path.exists():
        logger.error(f"Config file '{config_file}' not found.")
        _create_example_config(Path("config.ini")) # Create example in cwd
        sys.exit(1)

    try:
        logger.info(f"Loading config from: {config_file}")
        config.read(config_path, encoding="utf-8")

        # --- Database Auth Config ---
        if "Database" not in config:
            logger.error("Missing [Database] section.")
            _create_example_config(config_path, False)
            sys.exit(1)
        db_sec = config["Database"]
        auth_method = db_sec.get("auth_method", "windows").lower().strip()
        allowed_auth = ["windows", "sql", "service_principal", "interactive"]
        if auth_method not in allowed_auth:
            logger.error(f"Invalid auth_method '{auth_method}'. Use: {allowed_auth}")
            sys.exit(1)
        auth_cfg = AuthConfig(
            method=auth_method,
            username=db_sec.get("sql_username"),
            password=db_sec.get("sql_password"),
            client_id=db_sec.get("client_id"),
            tenant_id=db_sec.get("tenant_id"),
            client_secret=db_sec.get("client_secret"),
        )
        # Validate required auth fields
        if auth_method == "service_principal" and not all(
            [auth_cfg.client_id, auth_cfg.tenant_id, auth_cfg.client_secret]
        ):
            logger.error("SP auth needs client_id, tenant_id, client_secret.")
            sys.exit(1)
        if auth_method == "interactive" and not all(
            [auth_cfg.client_id, auth_cfg.tenant_id]
        ):
            logger.error("Interactive auth needs client_id, tenant_id.")
            # Consider allowing interactive without client_id/tenant_id if using integrated driver auth?
            # For now, assume MSAL interactive needs them.
        if auth_method == "sql" and not all(
            [auth_cfg.username, auth_cfg.password is not None]
        ):
            logger.error("SQL auth needs sql_username, sql_password.")
            sys.exit(1)

        # --- DEFAULT User Input Config ---
        if "DEFAULT" not in config:
            logger.error("Missing [DEFAULT] section.")
            _create_example_config(config_path, False)
            sys.exit(1)
        def_sec = config["DEFAULT"]
        try:
            sensitive_flag = def_sec.getint("is_sensitive", DEFAULT_SENSITIVE_FLAG)
        except ValueError:
            logger.warning(
                f"Invalid 'is_sensitive'. Default: {DEFAULT_SENSITIVE_FLAG}"
            )
            sensitive_flag = DEFAULT_SENSITIVE_FLAG
        try:
            archive_keep = def_sec.getint(
                "archive_keep_count", DEFAULT_ARCHIVE_KEEP_COUNT
            )
            if archive_keep < 0:
                logger.warning(
                    f"Negative archive_keep_count. Default: {DEFAULT_ARCHIVE_KEEP_COUNT}"
                )
                archive_keep = DEFAULT_ARCHIVE_KEEP_COUNT
        except ValueError:
            logger.warning(
                f"Invalid 'archive_keep_count'. Default: {DEFAULT_ARCHIVE_KEEP_COUNT}"
            )
            archive_keep = DEFAULT_ARCHIVE_KEEP_COUNT
        user_input_cfg = UserInputConfig(
            created_by=def_sec.get("user", "SchemaExporterScript"),
            source_name=def_sec.get("source_name", "Automated Extraction"),
            business_domain=def_sec.get("business_domain", "Unknown"),
            business_owner=def_sec.get("business_owner", "Unknown"),
            is_sensitive=sensitive_flag,
            archive_keep_count=archive_keep,
        )

        # --- Domain Configs ---
        domains = []
        domain_sections = [s for s in config.sections() if s.startswith("Domain.")]
        if not domain_sections:
            logger.error("No [Domain.*] sections found.")
            _create_example_config(config_path, False)
            sys.exit(1)

        for section in domain_sections:
            name = section.split(".", 1)[1]
            domain_sec = config[section]
            sql_server = domain_sec.get("sql_server")
            sharepoint_path_str = domain_sec.get("sharepoint_path")
            sp_site_url = domain_sec.get("sharepoint_site_url") # For link generation
            sp_doclib_path = domain_sec.get("sharepoint_doc_library_path") # For link generation

            if not sql_server or not sharepoint_path_str:
                logger.error(
                    f"Domain '{name}' missing sql_server/sharepoint_path. Skipping."
                )
                continue
            try:
                sp_path = Path(sharepoint_path_str)
                if not sp_path.is_absolute() and not sharepoint_path_str.startswith(
                    r"\\"
                ):
                    logger.warning(
                        f"Domain '{name}': sharepoint_path '{sharepoint_path_str}' not absolute."
                    )
            except Exception as path_e:
                logger.error(
                    f"Domain '{name}': Invalid sharepoint_path '{sharepoint_path_str}': {path_e}. Skipping."
                )
                continue

            # Warn if SP link info missing
            if not sp_site_url or not sp_doclib_path:
                logger.warning(
                    f"Domain '{name}': Missing SP base URLs in config. Links cannot be generated."
                )

            db_override = [
                db.strip()
                for db in domain_sec.get("db_override", "").split(",")
                if db.strip()
            ]
            db_prefix = domain_sec.get("db_prefix")
            if not db_override and not db_prefix:
                logger.warning(f"Domain '{name}': No db_override/db_prefix specified.")

            domains.append(
                DomainConfig(
                    name=name,
                    sql_server=sql_server.strip(),
                    sharepoint_path=sp_path,
                    sharepoint_site_url=sp_site_url.strip("/") if sp_site_url else None,
                    sharepoint_doc_library_path=sp_doclib_path.strip("/") if sp_doclib_path else None, # Store the segment
                    db_prefix=db_prefix.strip() if db_prefix else None,
                    db_override=db_override,
                )
            )

        if not domains and domain_sections:
            logger.error("No *valid* [Domain.*] sections loaded.")
            sys.exit(1)
        elif not domains and not domain_sections: # Should be caught earlier
             logger.error("No [Domain.*] sections defined.")
             sys.exit(1)

        logger.info(f"Loaded {len(domains)} valid domain configurations.")
        logger.info(f"Auth method: {auth_cfg.method}")
        logger.info(f"Default User: {user_input_cfg.created_by}")
        logger.info(f"Default Archive Keep: {user_input_cfg.archive_keep_count}")

        # --- Flow URLs (HTTP Trigger URLs) ---
        flow_urls: Dict[str, str] = {}
        for section in config.sections():
            if section.startswith("Flow."):
                domain_key = section.split(".", 1)[1]
                trigger_url = config[section].get("notification_trigger_url") # Use new key
                if trigger_url:
                    if not trigger_url.startswith(("http://", "https://")):
                        logger.warning(
                            f"Notification URL for '{domain_key}' invalid: '{trigger_url}'."
                        )
                    flow_urls[domain_key] = trigger_url
                else:
                    logger.warning(f"No 'notification_trigger_url' found in [{section}].")
        logger.info(f"Loaded HTTP Notification Trigger URLs for {len(flow_urls)} domains.")

        return domains, user_input_cfg, auth_cfg, flow_urls

    except configparser.Error as e:
        logger.error(f"Error parsing config '{config_file}': {e}")
        sys.exit(1)
    except SystemExit:
        raise
    except Exception as e:
        logger.exception(f"Unexpected config loading error: {e}", exc_info=True)
        sys.exit(1)


def _create_example_config(config_path: Path, overwrite=True):
    """Creates a default/example config file reflecting current structure."""
    if config_path.exists() and not overwrite:
        logger.warning(f"Example config '{config_path}' exists. Skipping.")
        return

    config = configparser.ConfigParser(allow_no_value=True)
    # Add sections and keys using the dictionary approach for clarity
    config["DEFAULT"] = {
        "; Defaults": None,
        "user": "SchemaExporterScript",
        "source_name": "Automated Extraction",
        "business_domain": "Unknown", # Not used in output anymore
        "business_owner": "Unknown", # Not used in output anymore
        "is_sensitive": "1", # Not used in output anymore
        "; Sensitivity (1=T, 0=F) - Not used in output anymore": None,
        "archive_keep_count": str(DEFAULT_ARCHIVE_KEEP_COUNT),
        "; Archives to keep per DB": None,
    }
    config["Database"] = {
        "; Auth: windows, sql, service_principal, interactive": None,
        "auth_method": "windows",
        "; SQL Auth": None,
        "sql_username": "",
        "sql_password": "",
        "; AAD Auth": None,
        "client_id": "",
        "tenant_id": "",
        "; SP Only": None,
        "client_secret": "",
    }
    config["Domain.Example_SQL_Server"] = {
        "; Display name": None,
        "sql_server": "your_server.database.windows.net",
        "; Local/Network path to the root sync folder for this domain": None,
        "sharepoint_path": r"C:\Path\To\Sync\Root\DomainFolder",
        "; --- SP URLs for Links (Optional but needed for links in notifications) ---": None,
        "sharepoint_site_url": "https://yourtenant.sharepoint.com/sites/YourSiteName",
        "; URL Encoded library path segment (e.g., Shared%20Documents or Freigegebene%20Dokumente)": None,
        "sharepoint_doc_library_path": "Shared%20Documents",
        "; --- End New ---": None,
        "; Optional DB filtering:": None,
        "db_prefix": "Prod_",
        "db_override": "",
    }
    config["Domain.Example_Fabric"] = {
        "sql_server": "your_ws.datawarehouse.fabric.microsoft.com",
        "sharepoint_path": "/mnt/sp/sync/FabricDomain",
        "sharepoint_site_url": "https://yourtenant.sharepoint.com/sites/FabricSite",
        "sharepoint_doc_library_path": "Docs", # Example URL segment
        "db_prefix": "",
        "db_override": "Lakehouse1,WarehouseA",
    }
    config["Flow.Example_SQL_Server"] = {
        "; --- RENAMED KEY: Use the HTTP Trigger URL from Power Automate ---": None,
        "notification_trigger_url": "https://prod-....logic.azure.com:443/...",
    }
    config["Flow.Example_Fabric"] = {"notification_trigger_url": ""}

    try:
        with open(config_path, "w", encoding="utf-8") as f:
            f.write("# Config for AutoSchemaExtractor\n\n")
            config.write(f)
        logger.info(f"Example config created: {config_path}")
    except OSError as e:
        logger.error(f"Failed create example config {config_path}: {e}")


# ========== MAIN EXECUTION ==========
def main():
    """Main function: parse args, load config, process domains."""
    parser = argparse.ArgumentParser(
        description="Schema Exporter with Summary Notifications.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config.ini",
        help="Config INI file path (default: config.ini)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable DEBUG logging"
    )
    parser.add_argument(
        "--create-config",
        action="store_true",
        help="Create example config.ini and exit.",
    )
    args = parser.parse_args()

    # Setup logging based on args
    if args.verbose:
        setup_logging(logging.DEBUG)
        logger.info("Verbose logging enabled.")
    else:
        setup_logging(logging.INFO)

    # Handle config creation request
    if args.create_config:
        _create_example_config(Path("config.ini")) # Create in CWD
        sys.exit(0)

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"Schema Exporter Started: {start_time:%Y-%m-%d %H:%M:%S}")
    logger.info(f"Config: {args.config}")
    logger.info(f"Lock Method: {LOCK_METHOD}")
    logger.info("=" * 60)

    try:
        # Load configuration
        domains, user_input_cfg, auth_cfg, flow_urls = load_config(args.config)
        total_domains = len(domains)

        if total_domains == 0:
            logger.warning("No valid domains configured. Exiting.")
        else:
            logger.info(f"Starting processing for {total_domains} domain(s).")

        domain_errors = 0
        # Process each configured domain
        for i, domain_cfg in enumerate(domains, 1):
            logger.info(f"--- ({i}/{total_domains}) Processing Domain: {domain_cfg.name} ---")
            try:
                process_domain(domain_cfg, user_input_cfg, auth_cfg, flow_urls)
            except Exception as domain_e:
                logger.exception(
                    f"Unexpected error processing domain '{domain_cfg.name}': {domain_e}",
                    exc_info=True,
                )
                domain_errors += 1
                logger.error("Continuing to next domain...")

        # Log completion summary
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info("=" * 60)
        logger.info(f"Schema Exporter Finished: {end_time:%Y-%m-%d %H:%M:%S}")
        logger.info(f"Total execution time: {duration}")
        logger.info("=" * 60)

        if domain_errors > 0:
            logger.error(f"❌ Processing completed with errors in {domain_errors} domain(s).")
            sys.exit(1) # Exit with error code if domain processing failed
        elif total_domains > 0:
            logger.info("✅ All configured domains processed successfully.")
        else:
            logger.info("🏁 Script finished, no domains processed.")

    except SystemExit:
        logger.error("Exiting due to config errors or --create-config.")
    except Exception as e:
        logger.exception(f"Critical error in main execution: {e}", exc_info=True)
        sys.exit(1) # Exit with error code for critical failures


if __name__ == "__main__":
    # Dependencies: pandas, pyodbc, msal, requests
    main()