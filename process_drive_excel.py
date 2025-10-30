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
import requests.exceptions # JSONDecodeErrorを捕捉するためにインポート

# --- 設定項目 (GitHubの環境変数から自動で読み込む) ---
try:
    # 1. Google Drive 認証用 (ダウンロードで使用)
    sa_key_string = os.environ['GCP_SA_KEY']
    sa_key_json = json.loads(sa_key_string)
    
    # 2. フォルダID
    TARGET_EXCEL_FOLDER_ID = os.environ['INPUT_FOLDER_ID']
    UPLOAD_FOLDER_ID = os.environ['OUTPUT_FOLDER_ID'] # GASに渡すフォルダID
    
    # 3. Excelパスワード
    EXCEL_PASSWORD_1 = os.environ['EXCEL_PASSWORD_1']

    # 4. GAS Web App 連携用
    GAS_WEB_APP_URL = os.environ['GAS_WEB_APP_URL']
    GAS_SECRET_KEY = os.environ['GAS_SECRET_KEY'] # GASの 'SECRET_KEY' と一致させる

except KeyError as e:
    print(f"エラー: 必要な環境変数が設定されていません: {e}")
    print("GitHub Secrets/Variablesに以下が設定されているか確認してください:")
    print("'GCP_SA_KEY', 'INPUT_FOLDER_ID', 'OUTPUT_FOLDER_ID', 'EXCEL_PASSWORD_1', 'GAS_WEB_APP_URL', 'GAS_SECRET_KEY'")
    exit(1)

# Google APIのスコープ (Driveの読み取りのみ)
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


# 3. パスワード付きExcelを読み込む関数 (io.BytesIOを引数に取るよう修正)
def load_locked_excel(buffer: io.BytesIO, sheet_name: str, password: str) -> pd.DataFrame:
    """パスワード付きExcelファイル(メモリ上)を読み込む"""
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
        if "Decryption failed" in str(e) or "bad decrypt" in str(e):
            print(">>> パスワードが間違っているか、ファイル形式がサポートされていません。")
        return pd.DataFrame()

    return df


def output_secure_date() -> str:
    """暗号化コード実行日を返す"""
    today = datetime.date.today()
    return today.strftime("%y%m%d")


def main():
    """メインの処理を実行する関数"""
    
    # 1. 認証とサービスの準備 (Driveダウンロード用)
    print("Google Driveに認証中 (ダウンロード用)...")
    creds = service_account.Credentials.from_service_account_info(
        sa_key_json, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)

    # 2. 【入力】指定したフォルダ内の最新ファイルを取得
    FILE_PREFIX = "東大特進入学＆資料請求"
    print(f"入力フォルダ '{TARGET_EXCEL_FOLDER_ID}' 内で")
    print(f"プレフィックスが '{FILE_PREFIX}' の最新ファイルを検索中...")
    
    query = (
        f"'{TARGET_EXCEL_FOLDER_ID}' in parents "
        f"and trashed = false "
        f"and name starts with '{FILE_PREFIX}' "
    )
    # 共有ドライブからも読み取る場合 (必要に応じてコメント解除)
    # supports_all_drives = True
    # include_items_from_all_drives = True
    
    results = service.files().list(
        q=query,
        pageSize=1,
        orderBy='name desc', 
        fields='files(id, name)'
        # supportsAllDrives=supports_all_drives,
        # includeItemsFromAllDrives=include_items_from_all_drives
    ).execute()
    
    items = results.get('files', [])

    if not items:
        print('フォルダ内にファイルが見つかりませんでした。')
        return

    latest_file = items[0]
    file_id = latest_file['id']
    file_name = latest_file['name']
    print(f"最新ファイルが見つかりました: '{file_name}' (ID: {file_id})")

    # 3. ファイルをダウンロードしてメモリに読み込む
    request = service.files().get_media(fileId=file_id) # , supportsAllDrives=supports_all_drives)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f"ダウンロード中 {int(status.progress() * 100)}%")
    
    fh.seek(0)

    # 4. PandasでExcelデータを読み込む
    print(f"GAS SECRET_KEY: {GAS_SECRET_KEY}")  # デバッグ用にキーを出力
    print("PandasでExcelデータを読み込み中...")
    TARGET_SHEET_NAME = "H3(2026卒)"
    df: pd.DataFrame = load_locked_excel(fh, sheet_name=TARGET_SHEET_NAME, password=EXCEL_PASSWORD_1)
    
    if df.empty:
        print("Excelデータの読み込みに失敗したか、データが空です。処理を終了します。")
        return
    print("Excelデータの読み込み完了。")

    # 5. CSVデータをローカルではなくメモリ上に文字列として生成
    output_filename = f"secure-{output_secure_date()}_{file_name.replace('.xlsx', '')}.csv"
    print(f"'{output_filename}' のCSVデータをメモリ上に生成しました。")
    
    csv_data_string = df.to_csv(index=False, encoding='utf-8-sig')

    # 6. 【出力】Drive APIの代わりにGAS Web Appを呼び出す
    print("Google Apps Script Webアプリにアップロード中...")
    print(f"  -> フォルダID: {UPLOAD_FOLDER_ID}")
    print(f"  -> ファイル名: {output_filename}")

    # (★変更点★) 送信するキーの末尾4桁をログに出力
    if len(GAS_SECRET_KEY) > 4:
        print(f"  -> デバッグ: 送信するAPIキー: '...{GAS_SECRET_KEY[-4:]}'")
    else:
        print(f"  -> デバッグ: 送信するAPIキー: (短すぎるか、空です)")

    headers = {
        "X-Api-Key": GAS_SECRET_KEY
    }
    
    payload = {
        "folderId": UPLOAD_FOLDER_ID,
        "filename": output_filename,
        "csvData": csv_data_string
    }

    try:
        response = requests.post(GAS_WEB_APP_URL, headers=headers, json=payload, timeout=120)
        
        # HTTPステータスコードが4xx, 5xxの場合もエラーとして扱う
        response.raise_for_status() 

        # レスポンスがJSON形式であることを期待してパース
        response_json = response.json()

        if response_json.get("status") == "success":
            print("\n--- アップロード成功 ---")
            print(f"  ファイルID: {response_json.get('fileId')}")
            print(f"  ファイルURL: {response_json.get('fileUrl')}")
        else:
            # GASがエラーを報告した場合 (例: {status: "error", message: "..."})
            print("\n--- アップロード失敗 (GASがエラーを報告) ---")
            print(f"  メッセージ: {response_json.get('message')}")
            # ★変更点★ GASからの詳細なエラー(全ヘッダーなど)をそのまま出力
            print(f"  GASからの詳細: {response_json}")


    except requests.exceptions.JSONDecodeError as e:
        # GASがHTML(エラーページやログインページ)を返した場合
        print("\n--- 致命的エラー: GASからのレスポンスがJSONではありませんでした。 ---")
        print(f"  URL: {GAS_WEB_APP_URL}")
        print(f"  エラー: {e}")
        print(f"  受け取ったレスポンス (生テキスト): {response.text[:1000]}...") # レスポンスが長すぎるとログを埋めるため制限
        print("\n  >>> GASのデプロイ設定('全員'にアクセス許可)が正しいか確認してください。")
        print("  >>> もしHTML(Googleログインページ)が返されている場合、デプロイ設定が間違っています。")
    
    except requests.exceptions.RequestException as e:
        # ネットワークエラー、タイムアウト、HTTP 4xx/5xx エラーなど
        print(f"\n--- 致命的エラー: GAS Webアプリの呼び出しに失敗しました。 ---")
        print(f"  URL: {GAS_WEB_APP_URL}")
        print(f"  エラー: {e}")
        if e.response:
            print(f"  ステータスコード: {e.response.status_code}")
            print(f"  レスポンス: {e.response.text[:1000]}...")
        print("\n  >>> GASのURL、ネットワーク設定、またはGAS側のタイムアウトを確認してください。")

if __name__ == '__main__':
    main()

