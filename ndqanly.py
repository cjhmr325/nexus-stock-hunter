import yfinance as yf
import pandas as pd
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

def connect_to_sheet(sheet_url):
    # 깃허브 Secrets(금고)에 저장된 값을 읽어옵니다.
    json_data = os.environ.get("GOOGLE_SHEETS_JSON")
    
    if json_data:
        # 깃허브 서버에서 실행될 때 (금고 데이터 사용)
        info = json.loads(json_data)
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    else:
        # 내 컴퓨터에서 실행될 때 (로컬 파일 사용)
        creds = Credentials.from_service_account_file('secret_key.json', scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    
    gc = gspread.authorize(creds)
    return gc.open_by_url(sheet_url).worksheet("Raw_NQ")

def get_v9_metrics(ticker_symbol):
    tk = yf.Ticker(ticker_symbol)
    try:
        exps = tk.options
        if not exps: return None
        chain = tk.option_chain(exps[0])
        raw_list = []
        for df, side in [(chain.calls, 'Call'), (chain.puts, 'Put')]:
            tmp = df[df['openInterest'] > 0].copy()
            tmp['pos'] = (tmp['strike'] + tmp['lastPrice']) if side == 'Call' else (tmp['strike'] - tmp['lastPrice'])
            tmp['mass'] = tmp['lastPrice'] * tmp['openInterest'] * 100
            tmp['side'] = side
            raw_list.append(tmp)
        df_p = pd.concat(raw_list)
        t_mass = float(df_p['mass'].sum())
        top5 = df_p.nlargest(5, 'mass')
        t_center = float((top5['pos'] * top5['mass']).sum() / top5['mass'].sum())
        c_sum = float(df_p[df_p['side'] == 'Call']['mass'].sum())
        p_sum = float(df_p[df_p['side'] == 'Put']['mass'].sum())
        return {"pcr": p_sum/c_sum if c_sum > 0 else 0, "t_center": t_center, "dens": float(top5['mass'].sum()/t_mass), "t_mass": t_mass}
    except: return None

# [핵심 수정] 어떤 형태의 데이터든 순수 숫자로 강제 변환
def force_float(val):
    if isinstance(val, (pd.Series, pd.DataFrame)):
        if not val.empty:
            # 첫 번째 값을 꺼낸 후, 그것이 다시 시리즈라면 또 첫 번째 값을 꺼냄 (재귀적 추출)
            res = val.iloc[0]
            return force_float(res)
        return 0.0
    try:
        return float(val)
    except:
        return 0.0

def update_v10_final(raw_sheet):
    print("🚀 로컬 V10.3 엔진 가동: 타입 에러 완전 박멸 모드...")
    
    all_values = raw_sheet.get_all_values()
    existing_dates = [row[7] if len(row) > 7 else "" for row in all_values]
    
    last_row_idx = len(all_values)
    while last_row_idx > 0 and not any(all_values[last_row_idx-1]):
        last_row_idx -= 1

    # yfinance 라이브러리 특성에 따른 에러 방지용 auto_adjust=True
    s_df = yf.download("^NDX", period="5d", interval="1d", auto_adjust=True)
    f_h_df = yf.download("NQ=F", period="5d", interval="1h", auto_adjust=True)
    vxn_df = yf.download("^VXN", period="5d", interval="1d", auto_adjust=True)
    f_h_df.index = f_h_df.index.tz_localize(None)
    
    live_metrics = get_v9_metrics("^NDX")
    today_str = datetime.now().strftime('%Y-%m-%d')
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    for date, s_row in s_df.iterrows():
        curr_date = date.strftime('%Y-%m-%d')
        if curr_date not in [today_str, yesterday_str]: continue

        # [수정] s_row 내부 데이터 접근 방식 강화
        s_o = force_float(s_row['Open'])
        s_h = force_float(s_row['High'])
        s_l = force_float(s_row['Low'])
        s_c = force_float(s_row['Close'])
        s_v = int(force_float(s_row['Volume']))

        target_ts = date.replace(hour=16, minute=0)
        f_match = f_h_df[f_h_df.index <= target_ts].tail(1)
        
        if not f_match.empty:
            f_o = force_float(f_match['Open'])
            f_h = force_float(f_match['High'])
            f_l = force_float(f_match['Low'])
            f_c = force_float(f_match['Close'])
            f_v = int(force_float(f_match['Volume']))
        else: f_o = f_h = f_l = f_c = f_v = 0
            
        vxn = force_float(vxn_df.loc[date]['Close']) if date in vxn_df.index else 0
        is_today = (curr_date == today_str)

        row_data = [
            curr_date, round(s_o, 2), round(s_h, 2), round(s_l, 2), round(s_c, 2), s_v,
            round(f_o, 2), round(f_h, 2), round(f_l, 2), round(f_c, 2), f_v,
            round(f_c - s_c, 2), round(vxn, 2),
            int(live_metrics['t_mass']) if is_today else 0,
            round(live_metrics['pcr'], 2) if is_today else 0,
            round(live_metrics['t_center'], 2) if is_today else 0,
            round(live_metrics['dens'], 2) if is_today else 0,
            round(live_metrics['t_center'], 2) if is_today else 0,
            round((s_c - live_metrics['t_center'])/s_c * 100, 2) if is_today else 0
        ]

        if curr_date in existing_dates:
            target_idx = existing_dates.index(curr_date) + 1
            # 어제는 H~T(13개), 오늘은 H~Z(전체)
            raw_sheet.update(f'H{target_idx}:{"Z" if is_today else "T"}{target_idx}', [row_data if is_today else row_data[:13]])
            print(f"✅ {curr_date}: {target_idx}행 업데이트 완료")
        else:
            new_idx = last_row_idx + 1
            raw_sheet.update(f'H{new_idx}:Z{new_idx}', [row_data])
            last_row_idx += 1
            print(f"📌 {curr_date}: {new_idx}행 신규 추가")

if __name__ == "__main__":
    URL = "https://docs.google.com/spreadsheets/d/13oY7i3IWz8npmWsbqC9h9DYJPjAR2XN5XO3R8jlIurk/edit"
    try:
        raw_nq = connect_to_sheet(URL, json_key_path='secret_key.json')
        update_v10_final(raw_nq)
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
