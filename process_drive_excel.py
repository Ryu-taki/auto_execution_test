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
    UPLOAD_FOLDER_ID = os.environ['OUTPUT_FOLDER_ID'] # ← この値が正しいか要確認
    
    # 4. GitHub SecretsからExcelパスワードを読み込む
    EXCEL_PASSWORD_1 = os.environ['EXCEL_PASSWORD_1'] 

except KeyError as e:
    print(f"エラー: 必要な環境変数が設定されていません: {e}")
    print("GitHubのSecretsに 'GCP_SA_KEY'、 'EXCEL_PASSWORD_1'、ワークフローファイルに 'INPUT_FOLDER_ID' と 'OUTPUT_FOLDER_ID' が必要です。")
    exit(1)

# Google APIのスコープ
SCOPES = ['https://www.googleapis.com/auth/drive']


# 3. パスワード付きExcelを読み込む関数
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

    # (5. 処理 ...)
    
    # 6. 生成物をCSVファイルとしてローカル（GitHub Actionsの実行環境内）に保存
    OUTPUT_CSV_FILE_NAME = f"secure-{output_secure_date()}_{file_name.replace('.xlsx', '')}.csv"
    # Actionsのランナーの /tmp/ など一時ディレクトリに保存
    local_csv_path = f"/tmp/{OUTPUT_CSV_FILE_NAME}"
    df.to_csv(local_csv_path, index=False, encoding='utf-8-sig')
    print(f"\n'{local_csv_path}' として結果をローカルに保存しました。")
    
    # 7. 【出力】
    
    # --- ▼▼▼ デバッグコード追加 ▼▼▼ ---
    print("--- デバッグ情報 (アップロード先フォルダの確認) ---")
    try:
        # フォルダIDを使って、フォルダの情報を取得
        folder_info = service.files().get(
            fileId=UPLOAD_FOLDER_ID,
            fields='name, owners, capabilities' # フォルダ名、オーナー情報、権限情報をリクエスト
        ).execute()
        
        print(f"  フォルダ名: {folder_info.get('name')}")
        
        # 'owners' はリスト形式なので、各オーナーのメールアドレスを抽出
        owner_emails = [owner.get('emailAddress', 'N/A') for owner in folder_info.get('owners', [])]
        print(f"  フォルダのオーナー: {owner_emails}")
        
        # 'capabilities' から、サービスアカウントが子ファイルを追加できるか確認
        capabilities = folder_info.get('capabilities', {})
        print(f"  SAの権限 (canAddChildren): {capabilities.get('canAddChildren')}")
        
        # もしオーナーがあなたで、canAddChildrenがTrueなら、設定は完璧なはず
        if not capabilities.get('canAddChildren'):
             print("  警告: サービスアカウントはこのフォルダにファイルを追加(AddChildren)できません。")

    except Exception as e:
        print(f"  デバッグエラー: フォルダ情報の取得に失敗しました: {e}")
        print(f"  >>> 試行したID (OUTPUT_FOLDER_ID): {UPLOAD_FOLDER_ID}")
        print("  >>> ID が間違っているか、SAにこのフォルダへのアクセス権(閲覧権限すらない)がありません。")
    print("-------------------------------------------------")
    # --- ▲▲▲ デバッグコード追加 ▲▲▲ ---


    print(f"出力フォルダ '{UPLOAD_FOLDER_ID}' に '{OUTPUT_CSV_FILE_NAME}' を新規作成中...")
    file_metadata = {
        'name': OUTPUT_CSV_FILE_NAME, 
        'parents': [UPLOAD_FOLDER_ID] 
    }
    media = MediaFileUpload(local_csv_path, mimetype='text/csv') 

    try:
        upload_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        print(f"新規作成成功。ファイルID: {upload_file.get('id')}")

    except Exception as e:
        print(f"致命的エラー: 新規作成(create)に失敗しました。エラー: {e}")
        print("--- デバッグ情報 ---")
        print(f"試行したフォルダID (OUTPUT_FOLDER_ID): {UPLOAD_FOLDER_ID}")
        print(f"試行したファイル名: {OUTPUT_CSV_FILE_NAME}")
        print("Google Drive上でこのフォルダIDが正しいか、サービスアカウントに「編集者」権限があるか確認してください。")


if __name__ == '__main__':
    main()
