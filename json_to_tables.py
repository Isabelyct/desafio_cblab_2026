import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# =========================================================
# CONFIGURAÇÕES
# =========================================================

DATABASE_NAME = "restaurant_erp.db"
JSON_FILE = "ERP.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

LOGGER = logging.getLogger(__name__)


# =========================================================
# SQL QUERIES
# =========================================================

CREATE_TABLES_SCRIPT = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS restaurant_location (
    loc_ref TEXT PRIMARY KEY,
    cur_utc TEXT
);

CREATE TABLE IF NOT EXISTS guest_check (
    guest_check_id INTEGER PRIMARY KEY,
    loc_ref TEXT NOT NULL,
    chk_num INTEGER,
    opn_bus_dt TEXT,
    opn_utc TEXT,
    opn_lcl TEXT,
    clsd_bus_dt TEXT,
    clsd_utc TEXT,
    clsd_lcl TEXT,
    last_trans_utc TEXT,
    last_trans_lcl TEXT,
    last_updated_utc TEXT,
    last_updated_lcl TEXT,
    clsd_flag INTEGER,
    gst_cnt INTEGER,
    sub_ttl REAL,
    non_txbl_sls_ttl REAL,
    chk_ttl REAL,
    dsc_ttl REAL,
    pay_ttl REAL,
    bal_due_ttl REAL,
    rvc_num INTEGER,
    ot_num INTEGER,
    oc_num INTEGER,
    tbl_num INTEGER,
    tbl_name TEXT,
    emp_num INTEGER,
    num_srvc_rd INTEGER,
    num_chk_prntd INTEGER,

    FOREIGN KEY (loc_ref)
        REFERENCES restaurant_location(loc_ref)
);

CREATE TABLE IF NOT EXISTS guest_check_tax (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guest_check_id INTEGER NOT NULL,
    tax_num INTEGER,
    txbl_sls_ttl REAL,
    tax_coll_ttl REAL,
    tax_rate REAL,
    tax_type INTEGER,

    FOREIGN KEY (guest_check_id)
        REFERENCES guest_check(guest_check_id)
);

CREATE TABLE IF NOT EXISTS guest_check_detail_line (
    guest_check_line_item_id INTEGER PRIMARY KEY,
    guest_check_id INTEGER NOT NULL,
    line_type TEXT NOT NULL,
    rvc_num INTEGER,
    dtl_ot_num INTEGER,
    dtl_oc_num INTEGER,
    line_num INTEGER,
    dtl_id INTEGER,
    detail_utc TEXT,
    detail_lcl TEXT,
    last_update_utc TEXT,
    last_update_lcl TEXT,
    bus_dt TEXT,
    ws_num INTEGER,
    dsp_ttl REAL,
    dsp_qty REAL,
    agg_ttl REAL,
    agg_qty REAL,
    chk_emp_id INTEGER,
    chk_emp_num INTEGER,
    svc_rnd_num INTEGER,
    seat_num INTEGER,

    FOREIGN KEY (guest_check_id)
        REFERENCES guest_check(guest_check_id)
);

CREATE TABLE IF NOT EXISTS detail_line_menu_item (
    guest_check_line_item_id INTEGER PRIMARY KEY,
    mi_num INTEGER,
    mod_flag INTEGER,
    incl_tax REAL,
    active_taxes TEXT,
    prc_lvl INTEGER,

    FOREIGN KEY (guest_check_line_item_id)
        REFERENCES guest_check_detail_line(guest_check_line_item_id)
);

CREATE TABLE IF NOT EXISTS detail_line_discount (
    guest_check_line_item_id INTEGER PRIMARY KEY,
    discount_num INTEGER,
    discount_name TEXT,
    discount_amount REAL,
    discount_reason TEXT,

    FOREIGN KEY (guest_check_line_item_id)
        REFERENCES guest_check_detail_line(guest_check_line_item_id)
);

CREATE TABLE IF NOT EXISTS detail_line_service_charge (
    guest_check_line_item_id INTEGER PRIMARY KEY,
    service_charge_num INTEGER,
    service_charge_name TEXT,
    service_charge_amount REAL,

    FOREIGN KEY (guest_check_line_item_id)
        REFERENCES guest_check_detail_line(guest_check_line_item_id)
);

CREATE TABLE IF NOT EXISTS detail_line_tender_media (
    guest_check_line_item_id INTEGER PRIMARY KEY,
    tender_media_num INTEGER,
    tender_media_name TEXT,
    payment_amount REAL,
    payment_type TEXT,

    FOREIGN KEY (guest_check_line_item_id)
        REFERENCES guest_check_detail_line(guest_check_line_item_id)
);

CREATE TABLE IF NOT EXISTS detail_line_error_code (
    guest_check_line_item_id INTEGER PRIMARY KEY,
    error_code TEXT,
    error_message TEXT,

    FOREIGN KEY (guest_check_line_item_id)
        REFERENCES guest_check_detail_line(guest_check_line_item_id)
);
"""


INSERT_LOCATION_QUERY = """
INSERT OR IGNORE INTO restaurant_location (
    loc_ref,
    cur_utc
) VALUES (?, ?)
"""


INSERT_GUEST_CHECK_QUERY = """
INSERT OR REPLACE INTO guest_check (
    guest_check_id,
    loc_ref,
    chk_num,
    opn_bus_dt,
    opn_utc,
    opn_lcl,
    clsd_bus_dt,
    clsd_utc,
    clsd_lcl,
    last_trans_utc,
    last_trans_lcl,
    last_updated_utc,
    last_updated_lcl,
    clsd_flag,
    gst_cnt,
    sub_ttl,
    non_txbl_sls_ttl,
    chk_ttl,
    dsc_ttl,
    pay_ttl,
    bal_due_ttl,
    rvc_num,
    ot_num,
    oc_num,
    tbl_num,
    tbl_name,
    emp_num,
    num_srvc_rd,
    num_chk_prntd
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


INSERT_TAX_QUERY = """
INSERT INTO guest_check_tax (
    guest_check_id,
    tax_num,
    txbl_sls_ttl,
    tax_coll_ttl,
    tax_rate,
    tax_type
) VALUES (?, ?, ?, ?, ?, ?)
"""


INSERT_DETAIL_LINE_QUERY = """
INSERT OR REPLACE INTO guest_check_detail_line (
    guest_check_line_item_id,
    guest_check_id,
    line_type,
    rvc_num,
    dtl_ot_num,
    dtl_oc_num,
    line_num,
    dtl_id,
    detail_utc,
    detail_lcl,
    last_update_utc,
    last_update_lcl,
    bus_dt,
    ws_num,
    dsp_ttl,
    dsp_qty,
    agg_ttl,
    agg_qty,
    chk_emp_id,
    chk_emp_num,
    svc_rnd_num,
    seat_num
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# =========================================================
# CONSTANTES
# =========================================================

LINE_TYPES = {
    "menuItem": "MENU_ITEM",
    "discount": "DISCOUNT",
    "serviceCharge": "SERVICE_CHARGE",
    "tenderMedia": "TENDER_MEDIA",
    "errorCode": "ERROR_CODE"
}


# =========================================================
# FUNÇÕES UTILITÁRIAS
# =========================================================

def get_value(
    data: Dict[str, Any],
    key: str,
    default: Optional[Any] = None
) -> Any:
    return data.get(key, default)


def execute_query(
    conn: sqlite3.Connection,
    query: str,
    values: Tuple[Any, ...]
) -> None:
    conn.execute(query, values)


def identify_line_type(detail_line: Dict[str, Any]) -> str:
    for key, line_type in LINE_TYPES.items():
        if key in detail_line:
            return line_type

    return "UNKNOWN"


# =========================================================
# DATABASE
# =========================================================

def create_tables(conn: sqlite3.Connection) -> None:
    LOGGER.info("Criando tabelas...")

    conn.executescript(CREATE_TABLES_SCRIPT)

    conn.commit()

    LOGGER.info("Tabelas criadas com sucesso.")


# =========================================================
# LOAD JSON
# =========================================================

def load_erp_data(json_path: str) -> Dict[str, Any]:

    path = Path(json_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {json_path}"
        )

    LOGGER.info("Carregando arquivo JSON...")

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


# =========================================================
# INSERTS
# =========================================================

def insert_location(
    conn: sqlite3.Connection,
    erp_data: Dict[str, Any]
) -> None:

    values = (
        erp_data["locRef"],
        erp_data["curUTC"]
    )

    execute_query(
        conn,
        INSERT_LOCATION_QUERY,
        values
    )


def insert_guest_check(
    conn: sqlite3.Connection,
    guest_check: Dict[str, Any],
    loc_ref: str
) -> None:

    values = (
        guest_check["guestCheckId"],
        loc_ref,
        get_value(guest_check, "chkNum"),
        get_value(guest_check, "opnBusDt"),
        get_value(guest_check, "opnUTC"),
        get_value(guest_check, "opnLcl"),
        get_value(guest_check, "clsdBusDt"),
        get_value(guest_check, "clsdUTC"),
        get_value(guest_check, "clsdLcl"),
        get_value(guest_check, "lastTransUTC"),
        get_value(guest_check, "lastTransLcl"),
        get_value(guest_check, "lastUpdatedUTC"),
        get_value(guest_check, "lastUpdatedLcl"),
        int(bool(get_value(guest_check, "clsdFlag", False))),
        get_value(guest_check, "gstCnt"),
        get_value(guest_check, "subTtl"),
        get_value(guest_check, "nonTxblSlsTtl"),
        get_value(guest_check, "chkTtl"),
        get_value(guest_check, "dscTtl"),
        get_value(guest_check, "payTtl"),
        get_value(guest_check, "balDueTtl"),
        get_value(guest_check, "rvcNum"),
        get_value(guest_check, "otNum"),
        get_value(guest_check, "ocNum"),
        get_value(guest_check, "tblNum"),
        get_value(guest_check, "tblName"),
        get_value(guest_check, "empNum"),
        get_value(guest_check, "numSrvcRd"),
        get_value(guest_check, "numChkPrntd")
    )

    execute_query(
        conn,
        INSERT_GUEST_CHECK_QUERY,
        values
    )


def insert_taxes(
    conn: sqlite3.Connection,
    guest_check: Dict[str, Any]
) -> None:

    taxes_values: List[Tuple[Any, ...]] = []

    for tax in guest_check.get("taxes", []):

        values = (
            guest_check["guestCheckId"],
            get_value(tax, "taxNum"),
            get_value(tax, "txblSlsTtl"),
            get_value(tax, "taxCollTtl"),
            get_value(tax, "taxRate"),
            get_value(tax, "type")
        )

        taxes_values.append(values)

    conn.executemany(
        INSERT_TAX_QUERY,
        taxes_values
    )


def insert_detail_line(
    conn: sqlite3.Connection,
    detail_line: Dict[str, Any],
    guest_check_id: int
) -> None:

    values = (
        detail_line["guestCheckLineItemId"],
        guest_check_id,
        identify_line_type(detail_line),
        get_value(detail_line, "rvcNum"),
        get_value(detail_line, "dtlOtNum"),
        get_value(detail_line, "dtlOcNum"),
        get_value(detail_line, "lineNum"),
        get_value(detail_line, "dtlId"),
        get_value(detail_line, "detailUTC"),
        get_value(detail_line, "detailLcl"),
        get_value(detail_line, "lastUpdateUTC"),
        get_value(detail_line, "lastUpdateLcl"),
        get_value(detail_line, "busDt"),
        get_value(detail_line, "wsNum"),
        get_value(detail_line, "dspTtl"),
        get_value(detail_line, "dspQty"),
        get_value(detail_line, "aggTtl"),
        get_value(detail_line, "aggQty"),
        get_value(detail_line, "chkEmpId"),
        get_value(detail_line, "chkEmpNum"),
        get_value(detail_line, "svcRndNum"),
        get_value(detail_line, "seatNum")
    )

    execute_query(
        conn,
        INSERT_DETAIL_LINE_QUERY,
        values
    )


# =========================================================
# PROCESSAMENTO
# =========================================================

def process_erp_json(
    conn: sqlite3.Connection,
    erp_data: Dict[str, Any]
) -> None:

    LOGGER.info("Iniciando processamento do ERP...")

    insert_location(conn, erp_data)

    loc_ref = erp_data["locRef"]

    for guest_check in erp_data.get("guestChecks", []):

        insert_guest_check(
            conn,
            guest_check,
            loc_ref
        )

        insert_taxes(
            conn,
            guest_check
        )

        for detail_line in guest_check.get("detailLines", []):

            insert_detail_line(
                conn,
                detail_line,
                guest_check["guestCheckId"]
            )

    conn.commit()

    LOGGER.info("Processamento concluído com sucesso.")


# =========================================================
# EXIBIÇÃO
# =========================================================

def show_loaded_data(conn: sqlite3.Connection) -> None:

    cursor = conn.cursor()

    queries = {
        "Lojas": """
            SELECT *
            FROM restaurant_location
        """,

        "Pedidos": """
            SELECT
                guest_check_id,
                loc_ref,
                chk_num,
                chk_ttl,
                pay_ttl
            FROM guest_check
        """,

        "Impostos": """
            SELECT
                guest_check_id,
                tax_num,
                tax_coll_ttl,
                tax_rate
            FROM guest_check_tax
        """
    }

    for title, query in queries.items():

        print(f"\n--- {title} ---")

        rows = cursor.execute(query).fetchall()

        if not rows:
            print("Nenhum registro encontrado.")
            continue

        columns = [
            description[0]
            for description in cursor.description
        ]

        print(columns)

        for row in rows:
            print(row)


# =========================================================
# MAIN
# =========================================================

def main() -> None:

    try:

        erp_data = load_erp_data(JSON_FILE)

        with sqlite3.connect(DATABASE_NAME) as conn:

            create_tables(conn)

            process_erp_json(
                conn,
                erp_data
            )

            show_loaded_data(conn)

        LOGGER.info(
            f"Carga concluída com sucesso no banco: {DATABASE_NAME}"
        )

    except Exception as error:

        LOGGER.error(
            f"Erro ao processar o arquivo ERP: {error}"
        )


if __name__ == "__main__":
    main()
