import yfinance as yf
import pandas as pd
import gspread
import os
import json
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# [1] 데이터 타입 에러 방지용 강제 숫자 변환 함수
def force_float(val):
    if isinstance(val, (pd.Series, pd.DataFrame)):
        return force_float(val.iloc[0]) if not val.empty else 0.0
    try:
        return float(val)
    except:
        return 0.0

# [2] 구글 시트 연결 (깃허브/로컬 공용)
def connect_to_sheet(sheet_url):
    json_data = os.environ.get("GOOGLE_SHEETS_JSON")
    if json_data:
        info = json.loads(json_data)
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    else:
        creds = Credentials.from_service_account_file('secret_key.json', scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    return gspread.authorize(creds).open_by_url(sheet_url).worksheet("Raw_NQ")

# [3] 옵션 지표 계산
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

# [4] 메인 업데이트 로직
def run_update(raw_sheet):
    # 데이터 다운로드 (타입 에러 방지 위해 auto_adjust=True)
    s_df = yf.download("^NDX", period="5d", interval="1d", auto_adjust=True)
    f_h_df = yf.download("NQ=F", period="5d", interval="1h", auto_adjust=True)
    vxn_df = yf.download("^VXN", period="5d", interval="1d", auto_adjust=True)
    
    all_values = raw_sheet.get_all_values()
    existing_dates = [row[7] if len(row) > 7 else "" for row in all_values] # H열 기준
    
    live_metrics = get_v9_metrics("^NDX")
    today_str = datetime.now().strftime('%Y-%m-%d')

    for date, s_row in s_df.iterrows():
        curr_date = date.strftime('%Y-%m-%d')
        
        # 모든 데이터에 force_float 적용 (Series 에러 원천 차단)
        s_o, s_h, s_l, s_c = force_float(s_row['Open']), force_float(s_row['High']), force_float(s_row['Low']), force_float(s_row['Close'])
        s_v = int(force_float(s_row['Volume']))

        vxn = force_float(vxn_df.loc[date]['Close']) if date in vxn_df.index else 0
        
        # 선물 데이터 매칭
        target_ts = date.replace(hour=16, minute=0)
        f_match = f_h_df[f_h_df.index.tz_localize(None) <= target_ts.replace(tzinfo=None)].tail(1)
        if not f_match.empty:
            f_o, f_h, f_l, f_c = force_float(f_match['Open']), force_float(f_match['High']), force_float(f_match['Low']), force_float(f_match['Close'])
            f_v = int(force_float(f_match['Volume']))
        else: f_o = f_h = f_l = f_c = f_v = 0

        is_today = (curr_date == today_str)
        row_data = [
            curr_date, round(s_o, 2), round(s_h, 2), round(s_l, 2), round(s_c, 2), s_v,
            round(f_o, 2), round(f_h, 2), round(f_l, 2), round(f_c, 2), f_v,
            round(f_c - s_c, 2), round(vxn, 2)
        ]
        
        # 오늘 데이터라면 옵션 지표 추가
        if is_today and live_metrics:
            row_data += [
                int(live_metrics['t_mass']), round(live_metrics['pcr'], 2),
                round(live_metrics['t_center'], 2), round(live_metrics['dens'], 2),
                round(live_metrics['t_center'], 2), round((s_c - live_metrics['t_center'])/s_c * 100, 2)
            ]

        if curr_date in existing_dates:
            idx = existing_dates.index(curr_date) + 1
            raw_sheet.update(f'H{idx}:Z{idx}', [row_data + [0]*(19-len(row_data))])
        else:
            raw_sheet.append_row([None]*7 + row_data) # H열부터 시작하도록 앞에 빈칸 추가

if __name__ == "__main__":
    URL = "https://docs.google.com/spreadsheets/d/13oY7i3IWz8npmWsbqC9h9DYJPjAR2XN5XO3R8jlIurk/edit"
    try:
        sheet = connect_to_sheet(URL)
        run_update(sheet)
        print("✅ 업데이트 성공")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
