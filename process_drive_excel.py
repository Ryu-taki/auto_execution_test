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
import requests.exceptions # ★ JSONDecodeErrorを捕捉するためにインポート

# --- 設定項目 (省略) ---
try:
    sa_key_string = os.environ['GCP_SA_KEY']
    sa_key_json = json.loads(sa_key_string)
    TARGET_EXCEL_FOLDER_ID = os.environ['INPUT_FOLDER_ID']
    UPLOAD_FOLDER_ID = os.environ['OUTPUT_FOLDER_ID']
    EXCEL_PASSWORD_1 = os.environ['EXCEL_PASSWORD_1'] 
    GAS_WEB_APP_URL = os.environ['GAS_WEB_APP_URL']
    GAS_SECRET_KEY = os.environ['GAS_SECRET_KEY']
except KeyError as e:
    print(f"エラー: 必要な環境変数が設定されていません: {e}")
    exit(1)

# --- スコープ (省略) ---
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# --- load_locked_excel 関数 (変更なし) ---
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

# --- output_secure_date 関数 (変更なし) ---
def output_secure_date() -> str:
    """暗号化コード実行日を返す"""
    today = datetime.date.today()
    return today.strftime("%y%m%d")


def main():
    """メインの処理を実行する関数"""
    
    # ... (ステップ1〜5は変更なし) ...
    
    # 1. 認証とサービスの準備
    print("Google Driveに認証中 (ダウンロード用)...")
    creds = service_account.Credentials.from_service_account_info(
        sa_key_json, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)

    # 2. 【入力】最新ファイルを取得
    FILE_PREFIX = "東大特進入学＆資料請求"
    print(f"入力フォルダ '{TARGET_EXCEL_FOLDER_ID}' 内で '{FILE_PREFIX}' の最新ファイルを検索中...")
    query = (
        f"'{TARGET_EXCEL_FOLDER_ID}' in parents "
        f"and trashed = false "
        f"and name starts with '{FILE_PREFIX}' "
    )
    results = service.files().list(
        q=query, pageSize=1, orderBy='name desc', fields='files(id, name)'
    ).execute()
    items = results.get('files', [])
    if not items:
        print('フォルダ内にファイルが見つかりませんでした。')
        return
    latest_file = items[0]
    file_id = latest_file['id']
    file_name = latest_file['name']
    print(f"最新ファイルが見つかりました: '{file_name}' (ID: {file_id})")

    # 3. ファイルをダウンロード
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f"ダウンロード中 {int(status.progress() * 100)}%")
    fh.seek(0)

    # 4. PandasでExcelデータを読み込む
    print("PandasでExcelデータを読み込み中...")
    TARGET_SHEET_NAME = "H3(2026卒)"
    df: pd.DataFrame = load_locked_excel(fh, sheet_name=TARGET_SHEET_NAME, password=EXCEL_PASSWORD_1)
    if df.empty:
        print("Excelデータの読み込みに失敗したか、データが空です。処理を終了します。")
        return
    print("Excelデータの読み込み完了。")

    # 6. CSVデータをメモリ上に生成
    OUTPUT_CSV_FILE_NAME = f"secure-{output_secure_date()}_{file_name.replace('.xlsx', '')}.csv"
    csv_data_string = df.to_csv(index=False, encoding='utf-8-sig')
    print(f"\n'{OUTPUT_CSV_FILE_NAME}' のCSVデータをメモリ上に生成しました。")
    
    
    # 7. ★★★【出力】GAS WebアプリにCSVデータをPOSTする (エラーハンドリング強化) ★★★
    print(f"Google Apps Script Webアプリにアップロード中...")
    print(f"  -> フォルダID: {UPLOAD_FOLDER_ID}")
    print(f"  -> ファイル名: {OUTPUT_CSV_FILE_NAME}")

    payload = {
        "folderId": UPLOAD_FOLDER_ID,
        "filename": OUTPUT_CSV_FILE_NAME,
        "csvData": csv_data_string
    }
    headers = {
        "Content-Type": "application/json",
        "X-Api-Key": GAS_SECRET_KEY 
    }

    response_text = "" # デバッグ用にレスポンステキストを保持

    try:
        # (3) HTTP POSTリクエストを送信
        response = requests.post(GAS_WEB_APP_URL, headers=headers, json=payload, timeout=60)
        
        # ★ デバッグ用にレスポンステキストを先に取得
        response_text = response.text 

        # (4) レスポンスのステータスコードを確認
        response.raise_for_status() # 200番台以外の場合はエラーを送出
        
        # (5) GASからのJSONレスポンスをパース
        response_json = response.json()

        if response_json.get('status') == 'success':
            print("\n--- アップロード成功 (GAS) ---")
            print(f"  ファイル名: {response_json.get('fileName')}")
            print(f"  ファイルID: {response_json.get('fileId')}")
            print(f"  ファイルURL: {response_json.get('fileUrl')}")
        else:
            print(f"\n--- アップロード失敗 (GASがエラーを報告) ---")
            print(f"  メッセージ: {response_json.get('message')}")

    # ★ 変更点: JSONDecodeError を明示的にキャッチする
    except requests.exceptions.JSONDecodeError as json_err:
        print(f"\n--- 致命的エラー: GASからのレスポンスがJSONではありませんでした。 ---")
        print(f"  URL: {GAS_WEB_APP_URL}")
        print(f"  エラー: {json_err}")
        print(f"  受け取ったレスポンス (生テキスト): {response_text}")
        print("\n  >>> GASのデプロイ設定('全員'にアクセス許可)が正しいか確認してください。")
        print("  >>> もしHTML(Googleログインページ)が返されている場合、デプロイ設定が間違っています。")
        exit(1)

    except requests.exceptions.RequestException as e:
        print(f"\n--- 致命的エラー: GAS Webアプリの呼び出しに失敗しました。 ---")
        print(f"  URL: {GAS_WEB_APP_URL}")
        if e.response:
            print(f"  ステータスコード: {e.response.status_code}")
            try:
                print(f"  レスポンス (生テキスト): {e.response.text}") # エラー時も生テキストを出力
            except Exception:
                pass
        else:
            print(f"  エラー: {e}") # 接続エラーなど
        print("\n  >>> GASのURL、ネットワーク接続、SECRET_KEYが正しいか確認してください。")
        exit(1)


if __name__ == '__main__':
    main()
