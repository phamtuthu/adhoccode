import os
import requests
import csv
from io import StringIO
import re
from datetime import datetime, timedelta, timezone
from clickhouse_driver import Client

# --- Config từ biến môi trường hoặc ghi trực tiếp ---
APPSFLYER_TOKEN = os.environ.get('APPSFLYER_TOKEN')
APP_ID = os.environ.get('APP_ID')
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
    "Partner": "partner",
    "Media Source": "media_source",
    "Campaign": "campaign",
    "Adset": "adset",
    "Ad": "ad",
    "Ad Type": "ad_type",
    "Contributor 1 Touch Type": "contributor_1_touch_type",
    "Contributor 1 Touch Time": "contributor_1_touch_time",
    "Contributor 1 Partner": "contributor_1_partner",
    "Contributor 1 Match Type": "contributor_1_match_type",
    "Contributor 1 Media Source": "contributor_1_media_source",
    "Contributor 1 Campaign": "contributor_1_campaign",
    "Contributor 1 Engagement Type": "contributor_1_engagement_type",
    "Contributor 2 Touch Type": "contributor_2_touch_type",
    "Contributor 2 Touch Time": "contributor_2_touch_time",
    "Contributor 2 Partner": "contributor_2_partner",
    "Contributor 2 Media Source": "contributor_2_media_source",
    "Contributor 2 Campaign": "contributor_2_campaign",
    "Contributor 2 Match Type": "contributor_2_match_type",
    "Contributor 2 Engagement Type": "contributor_2_engagement_type",
    "Contributor 3 Touch Type": "contributor_3_touch_type",
    "Contributor 3 Touch Time": "contributor_3_touch_time",
    "Contributor 3 Partner": "contributor_3_partner",
    "Contributor 3 Media Source": "contributor_3_media_source",
    "Contributor 3 Campaign": "contributor_3_campaign",
    "Contributor 3 Match Type": "contributor_3_match_type",
    "Contributor 3 Engagement Type": "contributor_3_engagement_type",
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
    "Campaign ID": "campaignid"
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
    "attributed_touch_time", "install_time", "event_time",
    "contributor_1_touch_time", "contributor_2_touch_time",
    "contributor_3_touch_time", "device_download_time"
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
        from datetime import datetime
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    print(f"⚠️ DateTime sai định dạng: '{val}' -> set None")
    return None

def download_appsflyer_installs(from_time, to_time):
    url = (
        f"https://hq1.appsflyer.com/api/raw-data/export/app/{APP_ID}/installs_report/v5"
        f"?from={from_time}&to={to_time}&timezone=Asia%2FHo_Chi_Minh"
        f"&additional_fields={ADDITIONAL_FIELDS}"
    )
    headers = {"Authorization": APPSFLYER_TOKEN, "accept": "text/csv"}
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print("❌ Error:", resp.text)
        return []
    csvfile = StringIO(resp.text)
    reader = csv.DictReader(csvfile)
    # Remove BOM if exists
    reader.fieldnames = [h.strip('\ufeff') for h in reader.fieldnames]
    data = [row for row in reader]
    return data

def main():
    from_time = "2025-05-01 00:00:00"
    to_time   = "2025-05-31 23:59:59"
    print(f"🕒 Lấy AppsFlyer từ {from_time} đến {to_time} (Asia/Ho_Chi_Minh)")
    raw_data = download_appsflyer_installs(from_time, to_time)
    if not raw_data:
        print("⚠️ Không có data AppsFlyer trong khoảng này.")
        return

    # Chuẩn hóa cột và lấy đúng thứ tự mapping
    appsflyer_cols = list(APPSFLYER_TO_CH.keys())
    ch_cols = list(APPSFLYER_TO_CH.values())

    # Chuẩn hóa & map sang đúng format
    mapped_data = []
    for row in raw_data:
        mapped_row = []
        for af_col, ch_col in zip(appsflyer_cols, ch_cols):
            val = row.get(af_col)
            if ch_col in DATETIME_CH_COLS:
                mapped_row.append(parse_datetime(val))
            else:
                mapped_row.append(val if val not in (None, "", "null", "None") else None)
        mapped_data.append(mapped_row)

    # Query ClickHouse để lấy các appsflyer_id đã có trong khoảng from_time → to_time
    client = Client(
        host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASSWORD, database=CH_DATABASE
    )
    result = client.execute(
        f"SELECT appsflyer_id FROM {CH_TABLE} WHERE install_time >= '{from_time}' AND install_time <= '{to_time}'"
    )
    existed = set(str(r[0]) for r in result if r[0])
    print(f"🔎 Có {len(existed)} ID đã tồn tại trong ClickHouse.")

    # Lọc dòng mới
    afid_idx = ch_cols.index('appsflyer_id')
    new_rows = [row for row in mapped_data if row[afid_idx] and row[afid_idx] not in existed]
    print(f"➕ Số dòng mới sẽ insert: {len(new_rows)}")

    if new_rows:
        client.execute(
            f"INSERT INTO {CH_TABLE} ({', '.join(ch_cols)}) VALUES",
            new_rows
        )
        print("✅ Đã insert lên ClickHouse xong!")
    else:
        print("Không có dòng mới để insert.")

    client.disconnect()

if __name__ == "__main__":
    main()
