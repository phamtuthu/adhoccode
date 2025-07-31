import os
import requests
import csv
from io import StringIO
import re
from datetime import datetime, timedelta
from clickhouse_driver import Client

# --- Config ---
APPSFLYER_TOKEN = os.environ.get('APPSFLYER_TOKEN')
APP_IDS = os.environ.get('APP_IDS', 'id1203171490,vn.ghn.app.giaohangnhanh').split(',')
CH_HOST = os.environ.get('CH_HOST')
CH_PORT = int(os.environ.get('CH_PORT', 9000))
CH_USER = os.environ.get('CH_USER')
CH_PASSWORD = os.environ.get('CH_PASSWORD')
CH_DATABASE = os.environ.get('CH_DATABASE')
CH_TABLE = os.environ.get('CH_TABLE')

APPSFLYER_TO_CH = {
    "Attributed Touch Type": "attributed_touch_type",
    "Attributed Touch Time": "attributed_touch_time",
    "Install Time": "install_time",
    "Event Time": "event_time",
    "Event Name": "event_name",
    "Event Value": "event_value",
    "Event Revenue": "event_revenue",
    "Partner": "partner",
    "Media Source": "media_source",
    "Campaign": "campaign",
    "Adset": "adset",
    "Ad": "ad",
    "Ad Type": "ad_type",
    "City": "city",
    "IP": "ip",
    "AppsFlyer ID": "appsflyer_id",
    "Customer User ID": "customer_user_id",
    "IDFA": "idfa",
    "IDFV": "idfv",
    "Device Category": "device_category",
    "Platform": "platform",
    "OS Version": "os_version",
    "Bundle ID": "bundle_id",
    "Is Retargeting": "is_retargeting",
    "Attribution Lookback": "attribution_lookback",
    "Match Type": "match_type",
    "Device Download Time": "device_download_time",
    "Device Model": "device_model",
    "Engagement Type": "engagement_type",
    "Campaign ID": "campaignid",
}
ADDITIONAL_FIELDS = (
    'blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,custom_dimension,conversion_type,'
    'gp_click_time,match_type,mediation_network,oaid,deeplink_url,blocked_reason,blocked_sub_reason,'
    'gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,rejected_reason,device_download_time,'
    'keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,monetization_network,'
    'segment,is_lat,gp_referrer,blocked_reason_value,store_product_page,device_category,app_type,'
    'rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att,'
    'engagement_type,gdpr_applies,ad_user_data_enabled,ad_personalization_enabled'
)
DATETIME_CH_COLS = {
    "attributed_touch_time", "install_time", "event_time", "device_download_time"
}

def parse_datetime(val):
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ('', 'null', 'none', 'n/a'):
        return None
    if '.' in s:
        s = s.split('.')[0]
    match = re.match(r"^(\d{4}-\d{2}-\d{2}) (\d{1,2}):(\d{2}):(\d{2})$", s)
    if match:
        date_part, hour, minute, second = match.groups()
        hour = hour.zfill(2)
        s = f"{date_part} {hour}:{minute}:{second}"
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", s):
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    print(f"⚠️ DateTime sai định dạng: '{val}' -> set None")
    return None

def parse_int_zero(val):
    try:
        if val is None or str(val).strip() in ('', 'null', 'none', 'n/a'):
            return 0
        return int(float(val))
    except Exception:
        return 0

def download_appsflyer_events(app_id, from_time, to_time):
    url = (
        f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/in_app_events_report/v5"
        f"?from={from_time}&to={to_time}&timezone=Asia%2FHo_Chi_Minh"
        f"&additional_fields={ADDITIONAL_FIELDS}"
    )
    headers = {"Authorization": APPSFLYER_TOKEN, "accept": "text/csv"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"❌ Error ({app_id}):", resp.text)
        return []
    csvfile = StringIO(resp.text)
    reader = csv.DictReader(csvfile)
    reader.fieldnames = [h.strip('\ufeff') for h in reader.fieldnames]
    data = [row for row in reader]
    return data

def get_bundle_id(app_id):
    if app_id == "id1203171490":
        return "vn.ghn.app.shiip"
    return app_id

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def get_imported_days(client, table, start_date, end_date, bundle_id=None):
    query = f"""
        SELECT DISTINCT toDate(event_time) as event_date
        FROM {table}
        WHERE event_time >= '{start_date.strftime('%Y-%m-%d 00:00:00')}'
          AND event_time <= '{end_date.strftime('%Y-%m-%d 23:59:59')}'
    """
    if bundle_id:
        query += f" AND bundle_id = '{bundle_id}'"
    rows = client.execute(query)
    return set(row[0] for row in rows)

def main():
    # ==== Chỉnh sửa ngày ở đây ====
    start_date = datetime.strptime("2025-05-01", "%Y-%m-%d")
    end_date   = datetime.strptime("2025-05-10", "%Y-%m-%d")
    # ==============================

    print(f"🕒 Import AppsFlyer events từng ngày từ {start_date.date()} đến {end_date.date()} (Asia/Ho_Chi_Minh)")

    client = Client(
        host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASSWORD, database=CH_DATABASE
    )
    appsflyer_cols = list(APPSFLYER_TO_CH.keys())
    ch_cols = list(APPSFLYER_TO_CH.values())
    total_inserted = 0

    for app_id in APP_IDS:
        app_id = app_id.strip()
        bundle_id = get_bundle_id(app_id)
        print(f"\n==== Processing APP_ID: {app_id} (bundle_id={bundle_id}) ====")
        imported_days = get_imported_days(client, CH_TABLE, start_date, end_date, bundle_id)
        print(f"== Ngày đã có dữ liệu: {[str(x) for x in sorted(imported_days)]}")

        for single_date in daterange(start_date, end_date):
            day_str = single_date.date().isoformat()
            if single_date.date() in imported_days:
                print(f"-- Đã có dữ liệu ngày {day_str}, bỏ qua.")
                continue

            from_time = single_date.strftime("%Y-%m-%d 00:00:00")
            to_time   = single_date.strftime("%Y-%m-%d 23:59:59")
            print(f"\n-- Đang import ngày: {day_str}")
            raw_data = download_appsflyer_events(app_id, from_time, to_time)
            if not raw_data:
                print(f"⚠️ Không có data AppsFlyer cho app {app_id} ngày {day_str}.")
                continue

            mapped_data = []
            for row in raw_data:
                mapped_row = []
                for i, (af_col, ch_col) in enumerate(zip(appsflyer_cols, ch_cols)):
                    val = row.get(af_col)
                    if ch_col in DATETIME_CH_COLS:
                        dt_val = parse_datetime(val)
                        mapped_row.append(dt_val)
                    elif ch_col == "event_revenue":
                        mapped_row.append(parse_int_zero(val))
                    elif ch_col == "bundle_id":
                        mapped_row.append(bundle_id)
                    else:
                        mapped_row.append(val if val not in (None, "", "null", "None") else None)
                mapped_data.append(mapped_row)

            print(f"➕ Insert {len(mapped_data)} rows for ngày {day_str}")
            if mapped_data:
                client.execute(
                    f"INSERT INTO {CH_TABLE} ({', '.join(ch_cols)}) VALUES",
                    mapped_data
                )
                print(f"✅ Đã insert {len(mapped_data)} rows cho ngày {day_str}")
                total_inserted += len(mapped_data)
            else:
                print("Không có dòng mới để insert.")

    client.disconnect()
    print(f"\n== Tổng số rows import vào ClickHouse (cả {len(APP_IDS)} app): {total_inserted} ==")

if __name__ == "__main__":
    main()
