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
    UPLOAD_FOLDER_ID = os.environ['OUTPUT_FOLDER_ID'] # GASに渡すフォルダID
    
    # 3. Excelパスワード
    EXCEL_PASSWORD_1 = os.environ['EXCEL_PASSWORD_1']

    # 4. GAS Web App 連携用
    GAS_WEB_APP_URL = os.environ['GAS_WEB_APP_URL']
    GAS_SECRET_KEY = os.environ['GAS_SECRET_KEY'] # GASの 'SECRET_KEY' と一致させる

except KeyError as e:
    print(f"エラー: 必要な環境変数が設定されていません: {e}")
    # ... (エラーメッセージ) ...
    exit(1)

# Google APIのスコープ (Driveの読み取りのみ)
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


# 3. パスワード付きExcelを読み込む関数 (変更なし)
def load_locked_excel(buffer: io.BytesIO, sheet_name: str, password: str) -> pd.DataFrame:
    # ... (変更なし) ...
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
    # ... (変更なし) ...
    today = datetime.date.today()
    return today.strftime("%y%m%d")


def main():
    """メインの処理を実行する関数"""
    
    # 1. 認証とサービスの準備 (変更なし)
    print("Google Driveに認証中 (ダウンロード用)...")
    creds = service_account.Credentials.from_service_account_info(
        sa_key_json, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)

    # 2. 【入力】指定したフォルダ内の最新ファイルを取得 (変更なし)
    FILE_PREFIX = "東大特進入学＆資料請求"
    print(f"入力フォルダ '{TARGET_EXCEL_FOLDER_ID}' 内で")
    print(f"プレフィックスが '{FILE_PREFIX}' の最新ファイルを検索中...")
    
    query = (
        f"'{TARGET_EXCEL_FOLDER_ID}' in parents "
        f"and trashed = false "
        f"and name starts with '{FILE_PREFIX}' "
    )
    results = service.files().list(
        q=query,
        pageSize=1,
        orderBy='name desc', 
        fields='files(id, name, modifiedTime)'
    ).execute()
    
    items = results.get('files', [])

    if not items:
        print('フォルダ内にファイルが見つかりませんでした。')
        return
    # ... (ファイル名取得、ダウンロード、Excel読み込み、CSV生成 ... 変更なし) ...
    latest_file = items[0]
    file_id = latest_file['id']
    file_name = latest_file['name']
    stored_time = latest_file['modifiedTime']
    print(f"最新ファイルが見つかりました: '{file_name}' (更新日時: {stored_time})")

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f"ダウンロード中 {int(status.progress() * 100)}%")
    fh.seek(0)

    print("PandasでExcelデータを読み込み中...")
    for sheet_name in ["H1(2028卒)", "H2(2027卒)", "H3(2026卒)"]:
        print(f"-> シート名: {sheet_name}")
        df: pd.DataFrame = load_locked_excel(fh, sheet_name=sheet_name, password=EXCEL_PASSWORD_1)
        
        if df.empty:
            print("Excelデータの読み込みに失敗したか、データが空です。処理を終了します。")
            return
        print("Excelデータの読み込み完了。")

        output_filename = f"{output_secure_date()}/secure-{output_secure_date()}_{file_name.replace('.xlsx', '')}_{sheet_name}.csv"
        print(f"'{output_filename}' のCSVデータをメモリ上に生成しました。")
        csv_data_string = df.to_csv(index=False, encoding='utf-8-sig')


        # 6. 【出力】GAS Web Appを呼び出す
        success = upload_csv_to_gas(
            csv_data=csv_data_string,
            file_path=output_filename,
            folder_id=UPLOAD_FOLDER_ID,
            gas_url=GAS_WEB_APP_URL,
            gas_key=GAS_SECRET_KEY
        )
        
        if success:
            print("処理が正常に完了しました。")
        else:
            print("処理中にエラーが発生しました。")
            exit(1)


def upload_csv_to_gas(
    csv_data: str,
    file_path: str,
    folder_id: str,
    gas_url: str,
    gas_key: str
) -> bool:
    """csvデータをGAS Web AppにPOSTリクエストで送信し、Driveにアップロードする.
    
    Args:
        csv_data (str): アップロードするcsvデータの文字列
        file_path (str): GAS側で解釈されるファイルパス
        folder_id (str): Driveの親フォルダID
        gas_url (str): GAS Web AppのデプロイURL
        gas_key (str): GASと共有するシークレットキー

    Returns:
        bool: アップロードが成功したかどうか
    """
    print("Google Apps Script Webアプリにアップロード中...")
    print(f"-> フォルダID: {folder_id}")
    print(f"-> ファイル名: {file_path}")
    
    # GASが受け取るペイロードを定義
    payload = {
        "apiKey": gas_key,
        "folderId": folder_id,
        "filePath": file_path,
        "csvData": csv_data
    }
    
    try:
        response = requests.post(gas_url, json=payload, timeout=120)
        response.raise_for_status()  # HTTPエラーがあれば例外を発生させる
        response_json = response.json()

        if response_json.get("status") == "success":
            print("\n--- アップロード成功 ---")
            print(f"ファイルID: {response_json.get('fileId')}")
            print(f"ファイルURL: {response_json.get('fileUrl')}")
            return True
        else:
            print("\n--- アップロード失敗 (GASがエラーを報告) ---")
            print(f"メッセージ: {response_json.get('message')}")
            print(f"GASからの詳細: {response_json}")
            return False

    except requests.exceptions.JSONDecodeError as e:
        print("\n--- 致命的エラー: GASからのレスポンスがJSONではありませんでした。 ---")
        print(f"URL: {gas_url}")
        print(f"エラー: {e}")
        print(f"受け取ったレスポンス (生テキスト): {response.text[:1000]}...")
        print("\n>>> GASのデプロイ設定('全員'にアクセス許可)が正しいか確認してください。")
        return False

    except requests.exceptions.RequestException as e:
        print(f"\n--- 致命的エラー: GAS Webアプリの呼び出しに失敗しました。 ---")
        print(f"URL: {gas_url}")
        print(f"エラー: {e}")
        if e.response:
            print(f"ステータスコード: {e.response.status_code}")
            print(f"レスポンス: {e.response.text[:1000]}...")
        print("\n>>> GASのURL、ネットワーク設定、またはGAS側のタイムアウトを確認してください。")
        return False
    

if __name__ == '__main__':
    main()
