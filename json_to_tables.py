import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


DATABASE_NAME = "restaurant_erp.db"
JSON_FILE = "ERP.json"


def get_value(data: Dict[str, Any], key: str, default: Optional[Any] = None) -> Any:
    return data.get(key, default)


def create_tables(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    cursor.executescript("""
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

        FOREIGN KEY (loc_ref) REFERENCES restaurant_location(loc_ref)
    );

    CREATE TABLE IF NOT EXISTS guest_check_tax (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        guest_check_id INTEGER NOT NULL,
        tax_num INTEGER,
        txbl_sls_ttl REAL,
        tax_coll_ttl REAL,
        tax_rate REAL,
        tax_type INTEGER,

        FOREIGN KEY (guest_check_id) REFERENCES guest_check(guest_check_id)
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

        FOREIGN KEY (guest_check_id) REFERENCES guest_check(guest_check_id)
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
    """)

    conn.commit()


def identify_line_type(line: Dict[str, Any]) -> str:
    if "menuItem" in line:
        return "MENU_ITEM"
    if "discount" in line:
        return "DISCOUNT"
    if "serviceCharge" in line:
        return "SERVICE_CHARGE"
    if "tenderMedia" in line:
        return "TENDER_MEDIA"
    if "errorCode" in line:
        return "ERROR_CODE"
    return "UNKNOWN"


def insert_location(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    conn.execute("""
        INSERT OR IGNORE INTO restaurant_location (
            loc_ref,
            cur_utc
        ) VALUES (?, ?)
    """, (
        data["locRef"],
        data["curUTC"]
    ))


def insert_guest_check(
    conn: sqlite3.Connection,
    check: Dict[str, Any],
    loc_ref: str
) -> None:
    conn.execute("""
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
    """, (
        check["guestCheckId"],
        loc_ref,
        get_value(check, "chkNum"),
        get_value(check, "opnBusDt"),
        get_value(check, "opnUTC"),
        get_value(check, "opnLcl"),
        get_value(check, "clsdBusDt"),
        get_value(check, "clsdUTC"),
        get_value(check, "clsdLcl"),
        get_value(check, "lastTransUTC"),
        get_value(check, "lastTransLcl"),
        get_value(check, "lastUpdatedUTC"),
        get_value(check, "lastUpdatedLcl"),
        int(bool(get_value(check, "clsdFlag", False))),
        get_value(check, "gstCnt"),
        get_value(check, "subTtl"),
        get_value(check, "nonTxblSlsTtl"),
        get_value(check, "chkTtl"),
        get_value(check, "dscTtl"),
        get_value(check, "payTtl"),
        get_value(check, "balDueTtl"),
        get_value(check, "rvcNum"),
        get_value(check, "otNum"),
        get_value(check, "ocNum"),
        get_value(check, "tblNum"),
        get_value(check, "tblName"),
        get_value(check, "empNum"),
        get_value(check, "numSrvcRd"),
        get_value(check, "numChkPrntd")
    ))


def insert_taxes(conn: sqlite3.Connection, check: Dict[str, Any]) -> None:
    guest_check_id = check["guestCheckId"]

    for tax in check.get("taxes", []):
        conn.execute("""
            INSERT INTO guest_check_tax (
                guest_check_id,
                tax_num,
                txbl_sls_ttl,
                tax_coll_ttl,
                tax_rate,
                tax_type
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            guest_check_id,
            get_value(tax, "taxNum"),
            get_value(tax, "txblSlsTtl"),
            get_value(tax, "taxCollTtl"),
            get_value(tax, "taxRate"),
            get_value(tax, "type")
        ))


def insert_detail_line(
    conn: sqlite3.Connection,
    line: Dict[str, Any],
    guest_check_id: int
) -> None:
    line_type = identify_line_type(line)

    conn.execute("""
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
    """, (
        line["guestCheckLineItemId"],
        guest_check_id,
        line_type,
        get_value(line, "rvcNum"),
        get_value(line, "dtlOtNum"),
        get_value(line, "dtlOcNum"),
        get_value(line, "lineNum"),
        get_value(line, "dtlId"),
        get_value(line, "detailUTC"),
        get_value(line, "detailLcl"),
        get_value(line, "lastUpdateUTC"),
        get_value(line, "lastUpdateLcl"),
        get_value(line, "busDt"),
        get_value(line, "wsNum"),
        get_value(line, "dspTtl"),
        get_value(line, "dspQty"),
        get_value(line, "aggTtl"),
        get_value(line, "aggQty"),
        get_value(line, "chkEmpId"),
        get_value(line, "chkEmpNum"),
        get_value(line, "svcRndNum"),
        get_value(line, "seatNum")
    ))


def insert_menu_item(conn: sqlite3.Connection, line: Dict[str, Any]) -> None:
    menu_item = line.get("menuItem")

    if not menu_item:
        return

    conn.execute("""
        INSERT OR REPLACE INTO detail_line_menu_item (
            guest_check_line_item_id,
            mi_num,
            mod_flag,
            incl_tax,
            active_taxes,
            prc_lvl
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        line["guestCheckLineItemId"],
        get_value(menu_item, "miNum"),
        int(bool(get_value(menu_item, "modFlag", False))),
        get_value(menu_item, "inclTax"),
        get_value(menu_item, "activeTaxes"),
        get_value(menu_item, "prcLvl")
    ))


def insert_discount(conn: sqlite3.Connection, line: Dict[str, Any]) -> None:
    discount = line.get("discount")

    if not discount:
        return

    conn.execute("""
        INSERT OR REPLACE INTO detail_line_discount (
            guest_check_line_item_id,
            discount_num,
            discount_name,
            discount_amount,
            discount_reason
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        line["guestCheckLineItemId"],
        get_value(discount, "discountNum"),
        get_value(discount, "discountName"),
        get_value(discount, "discountAmount"),
        get_value(discount, "discountReason")
    ))


def insert_service_charge(conn: sqlite3.Connection, line: Dict[str, Any]) -> None:
    service_charge = line.get("serviceCharge")

    if not service_charge:
        return

    conn.execute("""
        INSERT OR REPLACE INTO detail_line_service_charge (
            guest_check_line_item_id,
            service_charge_num,
            service_charge_name,
            service_charge_amount
        ) VALUES (?, ?, ?, ?)
    """, (
        line["guestCheckLineItemId"],
        get_value(service_charge, "serviceChargeNum"),
        get_value(service_charge, "serviceChargeName"),
        get_value(service_charge, "serviceChargeAmount")
    ))


def insert_tender_media(conn: sqlite3.Connection, line: Dict[str, Any]) -> None:
    tender_media = line.get("tenderMedia")

    if not tender_media:
        return

    conn.execute("""
        INSERT OR REPLACE INTO detail_line_tender_media (
            guest_check_line_item_id,
            tender_media_num,
            tender_media_name,
            payment_amount,
            payment_type
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        line["guestCheckLineItemId"],
        get_value(tender_media, "tenderMediaNum"),
        get_value(tender_media, "tenderMediaName"),
        get_value(tender_media, "paymentAmount"),
        get_value(tender_media, "paymentType")
    ))


def insert_error_code(conn: sqlite3.Connection, line: Dict[str, Any]) -> None:
    error_code = line.get("errorCode")

    if not error_code:
        return

    conn.execute("""
        INSERT OR REPLACE INTO detail_line_error_code (
            guest_check_line_item_id,
            error_code,
            error_message
        ) VALUES (?, ?, ?)
    """, (
        line["guestCheckLineItemId"],
        get_value(error_code, "code"),
        get_value(error_code, "message")
    ))


def insert_detail_specific_data(conn: sqlite3.Connection, line: Dict[str, Any]) -> None:
    insert_menu_item(conn, line)
    insert_discount(conn, line)
    insert_service_charge(conn, line)
    insert_tender_media(conn, line)
    insert_error_code(conn, line)


def load_erp_data(json_path: str) -> Dict[str, Any]:
    path = Path(json_path)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {json_path}")

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def process_erp_json(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    insert_location(conn, data)

    loc_ref = data["locRef"]

    for check in data.get("guestChecks", []):
        insert_guest_check(conn, check, loc_ref)
        insert_taxes(conn, check)

        for line in check.get("detailLines", []):
            insert_detail_line(conn, line, check["guestCheckId"])
            insert_detail_specific_data(conn, line)

    conn.commit()


def show_loaded_data(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    queries = {
        "Lojas": "SELECT * FROM restaurant_location",
        "Pedidos": "SELECT guest_check_id, loc_ref, chk_num, chk_ttl, pay_ttl FROM guest_check",
        "Impostos": "SELECT guest_check_id, tax_num, tax_coll_ttl, tax_rate FROM guest_check_tax",
        "Linhas do pedido": "SELECT guest_check_line_item_id, guest_check_id, line_type, dsp_ttl, dsp_qty FROM guest_check_detail_line",
        "Itens de menu": "SELECT * FROM detail_line_menu_item"
    }

    for title, query in queries.items():
        print(f"\n--- {title} ---")
        rows = cursor.execute(query).fetchall()

        if not rows:
            print("Nenhum registro encontrado.")
            continue

        columns = [description[0] for description in cursor.description]
        print(columns)

        for row in rows:
            print(row)


def main() -> None:
    try:
        data = load_erp_data(JSON_FILE)

        with sqlite3.connect(DATABASE_NAME) as conn:
            create_tables(conn)
            process_erp_json(conn, data)
            show_loaded_data(conn)

        print(f"\nCarga concluída com sucesso no banco: {DATABASE_NAME}")

    except Exception as error:
        print(f"Erro ao processar o arquivo ERP: {error}")


if __name__ == "__main__":
    main()
