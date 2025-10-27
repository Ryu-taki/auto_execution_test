import io
import os
import datetime
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import json
import msoffcrypto

# --- 設定項目 (GitHubの環境変数から自動で読み込む) ---
try:
    # 1. GitHub SecretsからJSONキーの「文字列」を読み込む
    sa_key_string = os.environ['GCP_SA_KEY']
    # 2. 文字列をPythonが扱える辞書(JSON)形式に変換
    sa_key_json = json.loads(sa_key_string)
    
    # 3. GitHub ワークフローファイルからフォルダIDを読み込む
    TARGET_EXCEL_FOLDER_ID = os.environ['INPUT_FOLDER_ID']
    UPLOAD_FOLDER_ID = os.environ['OUTPUT_FOLDER_ID']
    
    # 4. GitHub SecretsからExcelパスワードを読み込む
    EXCEL_PASSWORD_1 = os.environ['EXCEL_PASSWORD_1'] # ← 2. 環境変数の読み込みを追加

except KeyError as e:
    print(f"エラー: 必要な環境変数が設定されていません: {e}")
    print("GitHubのSecretsに 'GCP_SA_KEY'、 'EXCEL_PASSWORD_1'、ワークフローファイルに 'INPUT_FOLDER_ID' と 'OUTPUT_FOLDER_ID' が必要です。")
    exit(1)

# Google APIのスコープ
SCOPES = ['https://www.googleapis.com/auth/drive']


# 3. パスワード付きExcelを読み込む関数 (io.BytesIOを引数に取るよう修正)
def load_locked_excel(buffer: io.BytesIO, sheet_name: str, password: str) -> pd.DataFrame:
    """パスワード付きExcelファイル(メモリ上)を読み込む

    Args:
        buffer (io.BytesIO): ダウンロードしたExcelファイルのバイナリデータ
        sheet_name (str): 読み込むシート名
        password (str): Excelファイルのパスワード

    Returns:
        pd.DataFrame: 読み込んだDataFrame
    """
    try:
        # メモリ上のBytesIOオブジェクトを直接msoffcryptoに渡す
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
        # msoffcrypto固有のエラーをキャッチ
        if "Decryption failed" in str(e) or "bad decrypt" in str(e):
            print(">>> パスワードが間違っているか、ファイル形式がサポートされていません。")
        return pd.DataFrame()

    return df


def output_secure_date() -> str:
    """暗号化コード実行日を返す

    Returns:
        str: 暗号化コード実行日（yymmdd）
    """
    today = datetime.date.today()

    return today.strftime("%y%m%d")


def main():
    """メインの処理を実行する関数"""
    
    # 1. 認証とサービスの準備
    print("Google Driveに認証中...")
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
    results = service.files().list(
        q=query,
        pageSize=1,
        orderBy='name desc', # 作成日の降順（新しい順）
        fields='files(id, name)'
    ).execute()
    
    items = results.get('files', [])

    if not items:
        print('フォルダ内にファイルが見つかりませんでした。')
        return

    latest_file = items[0]
    file_id = latest_file['id']
    file_name = latest_file['name']
    file_date = file_name[len(FILE_PREFIX):].strip()  # プレフィックス以降の日付部分を抽出
    print(f"最新ファイルが見つかりました: '{file_name}' (ID: {file_id})")

    # 3. ファイルをダウンロードしてメモリに読み込む
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
    print("--- 元データ (先頭5行) ---")
    print(df.head())

    # # 5. 必要な処理を施す (この部分はご自身の処理に書き換えてください)
    # #
    # # (例: '売上' 列がもしあれば、それに1.1をかける)
    # # if '売上' in df.columns:
    # #     df['税込売上'] = df['売上'] * 1.1
    # #
    
    # print("\n--- 処理後データ (先頭5行) ---")
    # print(df.head())

    # 6. 生成物をCSVファイルとしてローカル（GitHub Actionsの実行環境内）に保存
    # 出力するCSVファイルの名前

    OUTPUT_CSV_FILE_NAME = f"secure-{output_secure_date()}_{file_name.replace('.xlsx', '')}.csv"
    df.to_csv(OUTPUT_CSV_FILE_NAME, index=False, encoding='utf-8-sig')
    print(f"\n'{OUTPUT_CSV_FILE_NAME}' として結果をローカルに保存しました。")
    
    # 7. 【出力】デバッグのため、常にユニークな名前で「新規作成」を試みる
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_file_name = f"debug_output_{timestamp}.csv"

    print(f"デバッグ: 出力フォルダ '{UPLOAD_FOLDER_ID}' に '{debug_file_name}' を新規作成中...")
    file_metadata = {
        'name': debug_file_name,
        'parents': [UPLOAD_FOLDER_ID]
    }
    media = MediaFileUpload(OUTPUT_CSV_FILE_NAME, mimetype='text/csv')

    try:
        upload_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        print(f"デバッグ: 新規作成成功。ファイルID: {upload_file.get('id')}")

    except Exception as e:
        print(f"デバッグ: やはり新規作成(create)に失敗しました。エラー: {e}")

    # print(f"アップロード完了。ファイルID: {upload_file.get('id')}") # 元のコードの最後の行もコメントアウト

if __name__ == '__main__':
    main()
