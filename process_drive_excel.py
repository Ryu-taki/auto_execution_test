import io
import os
import datetime
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import json
import msoffcrypto
import requests
import requests.exceptions

# --- 設定項目 (GitHubの環境変数から自動で読み込む) ---
try:
    # 1. Google Drive 認証用 (ダウンロードで使用)
    sa_key_string = os.environ['GCP_SA_KEY']
    sa_key_json = json.loads(sa_key_string)
    
    # 2. フォルダID
    TARGET_EXCEL_FOLDER_ID = os.environ['INPUT_FOLDER_ID']
    UPLOAD_FOLDER_ID = os.environ['OUTPUT_FOLDER_ID']
    
    # 3. Excelパスワード
    EXCEL_PASSWORD_1 = os.environ['EXCEL_PASSWORD_1']

    # 4. GAS Web App 連携用
    GAS_WEB_APP_URL = os.environ['GAS_WEB_APP_URL']
    GAS_SECRET_KEY = os.environ['GAS_SECRET_KEY']

except KeyError as e:
    print(f"エラー: 必要な環境変数が設定されていません: {e}")
    exit(1)

# Google APIのスコープ (Driveの読み取りのみ)
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


# 3. パスワード付きExcelを読み込む関数 (変更なし)
def load_locked_excel(buffer: io.BytesIO, sheet_name: str, password: str) -> pd.DataFrame:
    try:
        office_file = msoffcrypto.OfficeFile(buffer)
        office_file.load_key(password=password)
        decrypted_buffer = io.BytesIO()
        office_file.decrypt(decrypted_buffer)
        decrypted_buffer.seek(0)
        df: pd.DataFrame = pd.read_excel(
            decrypted_buffer, sheet_name=sheet_name, engine="openpyxl"
        )
    except Exception as e:
        print(f"Error loading the locked Excel file: {e}")
        return pd.DataFrame()
    return df


def output_secure_date() -> str:
    today = datetime.date.today()
    return today.strftime("%y%m%d")


def main():
    """メインの処理を実行する関数"""
    
    # 1. 認証 (変更なし)
    print("Google Driveに認証中 (ダウンロード用)...")
    creds = service_account.Credentials.from_service_account_info(
        sa_key_json, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)

    # 2. 【入力】 (変更なし)
    FILE_PREFIX = "東大特進入学＆資料請求"
    print(f"入力フォルダ '{TARGET_EXCEL_FOLDER_ID}' 内で...")
    
    query = (
        f"'{TARGET_EXCEL_FOLDER_ID}' in parents "
        f"and trashed = false "
        f"and name starts with '{FILE_PREFIX}' "
    )
    results = service.files().list(
        q=query,
        pageSize=1,
        orderBy='name desc', 
        fields='files(id, name)'
    ).execute()
    
    items = results.get('files', [])

    if not items:
        print('フォルダ内にファイルが見つかりませんでした。')
        return
    latest_file = items[0]
    file_id = latest_file['id']
    file_name = latest_file['name']
    print(f"最新ファイルが見つかりました: '{file_name}' (ID: {file_id})")

    # 3. ダウンロード (変更なし)
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f"ダウンロード中 {int(status.progress() * 100)}%")
    fh.seek(0)

    # 4. Excel読み込み (変更なし)
    print("PandasでExcelデータを読み込み中...")
    TARGET_SHEET_NAME = "H3(2026卒)"
    df: pd.DataFrame = load_locked_excel(fh, sheet_name=TARGET_SHEET_NAME, password=EXCEL_PASSWORD_1)
    
    if df.empty:
        print("Excelデータの読み込みに失敗したか、データが空です。処理を終了します。")
        return
    print("Excelデータの読み込み完了。")

    # 5. CSVデータ生成 (変更なし)
    output_filename = f"secure-{output_secure_date()}_{file_name.replace('.xlsx', '')}.csv"
    print(f"'{output_filename}' のCSVデータをメモリ上に生成しました。")
    csv_data_string = df.to_csv(index=False, encoding='utf-8-sig')

    # 6. 【出力】GAS Web Appを呼び出す (v3: ヘッダー送信版)
    print("Google Apps Script Webアプリにヘッダーを送信してデバッグ中...")

    # ★★★ ここがデバッグ対象 ★★★
    # X-Api-Key ヘッダーを送信する
    headers = {
        "X-Api-Key": GAS_SECRET_KEY
    }
    
    # ペイロードにはキーを含めない
    payload = {
        "folderId": UPLOAD_FOLDER_ID,
        "filename": output_filename,
        "csvData": csv_data_string
    }

    try:
        # ★★★ ここがデバッグ対象 ★★★
        response = requests.post(GAS_WEB_APP_URL, headers=headers, json=payload, timeout=120)
        
        response.raise_for_status() 
        response_json = response.json()

        # GASが {status: "debug_info", ...} を返してくるはず
        print("\n--- デバッグ情報受信 ---")
        print(f"  GASからのメッセージ: {response_json.get('message')}")
        
        # ★★★ ここに全ヘッダーが出力されます ★★★
        print("  GASが受信した全ヘッダー:")
        import json # ログを見やすく整形
        print(json.dumps(response_json.get('received_headers'), indent=2))
        
        # 意図的にエラーとして終了 (デバッグ目的のため)
        print("\n[注意] デバッグが成功したため、ここで処理を停止します。")
        exit(1)

    except requests.exceptions.JSONDecodeError as e:
        print(f"\n--- 致命的エラー: GASがJSONを返しませんでした ---")
        print(f"  エラー: {e}")
        print(f"  受け取ったレスポンス (生テキスト): {response.text[:1000]}...")
    
    except requests.exceptions.RequestException as e:
        print(f"\n--- 致命的エラー: GAS Webアプリの呼び出しに失敗しました ---")
        print(f"  エラー: {e}")

if __name__ == '__main__':
    main()
