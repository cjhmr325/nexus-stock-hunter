import yfinance as yf
import pandas as pd
import gspread
import os
import json
import numpy as np
from datetime import datetime
from google.oauth2.service_account import Credentials

# [1] 데이터 타입 에러 방지
def force_float(val):
    if isinstance(val, (pd.Series, pd.DataFrame)):
        return force_float(val.iloc[0]) if not val.empty else 0.0
    try: return float(val)
    except: return 0.0

# [2] 구글 시트 연결
def connect_to_sheet(sheet_url):
    json_data = os.environ.get("GOOGLE_SHEETS_JSON")
    if json_data:
        info = json.loads(json_data)
        creds = Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    else:
        creds = Credentials.from_service_account_file('secret_key.json', scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    return gspread.authorize(creds).open_by_url(sheet_url).worksheet("Raw_NQ")

# [3] 10대 원천 데이터 추출 (Nexus Master Set)
def get_nexus_master_raw(ticker_symbol):
    tk = yf.Ticker(ticker_symbol)
    try:
        exps = tk.options
        if not exps: return [0]*10
        chain = None
        target_date = exps[0]
        for exp in exps[:2]:
            tmp_chain = tk.option_chain(exp)
            if not tmp_chain.calls.empty and tmp_chain.calls['openInterest'].sum() > 0:
                chain = tmp_chain
                target_date = exp
                break
        if chain is None: return [0]*10
        
        calls, puts = chain.calls, chain.puts
        c_active = calls[calls['openInterest'] > 0].copy()
        p_active = puts[puts['openInterest'] > 0].copy()

        c_oi_m = (c_active['strike'] * c_active['openInterest'] * 100).sum()
        p_oi_m = (p_active['strike'] * p_active['openInterest'] * 100).sum()
        c_vol_m = (c_active['strike'] * c_active['volume'].fillna(0) * 100).sum()
        p_vol_m = (p_active['strike'] * p_active['volume'].fillna(0) * 100).sum()
        c_avg_s = (c_active['strike'] * c_active['openInterest']).sum() / c_active['openInterest'].sum()
        p_avg_s = (p_active['strike'] * p_active['openInterest']).sum() / p_active['openInterest'].sum()
        avg_iv = (c_active['impliedVolatility'].mean() + p_active['impliedVolatility'].mean()) / 2
        top5_sum = c_active.nlargest(5, 'openInterest')['openInterest'].sum() + p_active.nlargest(5, 'openInterest')['openInterest'].sum()
        
        strikes = pd.concat([c_active['strike'], p_active['strike']]).unique()
        strikes = np.sort(strikes)[::5]
        def get_pain(s):
            return c_active[c_active['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum() + \
                   p_active[p_active['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
        pains = [get_pain(s) for s in strikes]
        max_pain = strikes[np.argmin(pains)] if len(pains) > 0 else 0
        dte = (datetime.strptime(target_date, '%Y-%m-%d') - datetime.now()).days

        return [int(c_oi_m), int(p_oi_m), int(c_vol_m), int(p_vol_m), round(float(max_pain), 2), int(top5_sum), round(float(c_avg_s), 2), round(float(p_avg_s), 2), round(float(avg_iv), 4), int(dte)]
    except: return [0]*10


# [4] 메인 업데이트 로직 (데이터 보존 로직 포함)
def run_update(raw_sheet):
    # 최신 가격 데이터 다운로드 (최근 5일)
    s_df = yf.download("^NDX", period="5d", interval="1d", auto_adjust=True)
    f_h_df = yf.download("NQ=F", period="5d", interval="1h", auto_adjust=True)
    vxn_df = yf.download("^VXN", period="5d", interval="1d", auto_adjust=True)
    
    # 시트의 기존 데이터를 모두 가져옴 (H열 날짜 확인 및 기존 Nexus 데이터 보존용)
    all_values = raw_sheet.get_all_values()
    # H열(Index 7)의 날짜 리스트 추출
    existing_dates = [row[7] if len(row) > 7 else "" for row in all_values] 
    
    # 오늘자 옵션 데이터 1회만 추출
    nexus_raw_today = get_nexus_master_raw("^NDX") 
    today_str = datetime.now().strftime('%Y-%m-%d')

    for date, s_row in s_df.iterrows():
        curr_date = date.strftime('%Y-%m-%d')
        
        # 1. 가격 세트 준비 (H열 ~ T열용 13개 항목)
        s_o, s_h, s_l, s_c = force_float(s_row['Open']), force_float(s_row['High']), force_float(s_row['Low']), force_float(s_row['Close'])
        s_v = int(force_float(s_row['Volume']))
        vxn = force_float(vxn_df.loc[date]['Close']) if date in vxn_df.index else 0
        
        target_ts = date.replace(hour=16, minute=0)
        f_match = f_h_df[f_h_df.index.tz_localize(None) <= target_ts.replace(tzinfo=None)].tail(1)
        if not f_match.empty:
            f_o, f_h, f_l, f_c = force_float(f_match['Open']), force_float(f_match['High']), force_float(f_match['Low']), force_float(f_match['Close'])
            f_v = int(force_float(f_match['Volume']))
        else: f_o = f_h = f_l = f_c = f_v = 0

        row_price = [curr_date, round(s_o, 2), round(s_h, 2), round(s_l, 2), round(s_c, 2), s_v, 
                     round(f_o, 2), round(f_h, 2), round(f_l, 2), round(f_c, 2), f_v, 
                     round(f_c - s_c, 2), round(vxn, 2)]

        # 2. 옵션 세트 결정 (U열 ~ AD열용 10개 항목)
        row_nexus = [0] * 10 # 기본값
        
        if curr_date in existing_dates:
            idx = existing_dates.index(curr_date)
            current_row_data = all_values[idx] if idx < len(all_values) else []
            
            # 오늘 날짜인 경우: 새로 추출한 데이터 사용
            if curr_date == today_str:
                row_nexus = nexus_raw_today
            # 오늘이 아닌 경우: 시트에 이미 데이터가 있다면 기존 값 유지 (0 덮어쓰기 방지)
            elif len(current_row_data) > 20:
                # U열(index 20)부터 AD열(index 29)까지 추출
                existing_nexus = current_row_data[20:30]
                # 데이터가 실질적으로 존재하는지 확인 (전부 '0'이나 빈칸이 아닌 경우)
                if any(str(val).strip() not in ["0", "0.0", ""] for val in existing_nexus):
                    row_nexus = existing_nexus
                else:
                    row_nexus = [0] * 10
        else:
            # 신규 날짜인 경우: 오늘이면 데이터 넣고, 아니면 0
            row_nexus = nexus_raw_today if (curr_date == today_str) else [0] * 10

        # 3. 최종 결합 및 업데이트
        final_row = row_price + row_nexus

        if curr_date in existing_dates:
            row_num = existing_dates.index(curr_date) + 1
            raw_sheet.update(f'H{row_num}:AD{row_num}', [final_row], value_input_option='USER_ENTERED')
            print(f"✅ {curr_date} 업데이트 완료 (가격 갱신 + 기존 옵션 데이터 보존)")
        else:
            # 새 행 삽입 로직
            new_idx = len([x for x in existing_dates if x.strip()]) + 1
            if new_idx < 61: new_idx = 61
            raw_sheet.update(f'H{new_idx}:AD{new_idx}', [final_row], value_input_option='USER_ENTERED')
            print(f"✅ {curr_date} 신규 삽입 완료 (H{new_idx})")
            
            # 리스트 갱신 (중복 방지)
            if len(existing_dates) < new_idx:
                existing_dates.extend([""] * (new_idx - len(existing_dates)))
            existing_dates[new_idx-1] = curr_date

if __name__ == "__main__":
    URL = "https://docs.google.com/spreadsheets/d/13oY7i3IWz8npmWsbqC9h9DYJPjAR2XN5XO3R8jlIurk/edit"
    try:
        sheet = connect_to_sheet(URL)
        run_update(sheet)
        print("🚀 모든 작업이 성공적으로 완료되었습니다.")
    except Exception as e: 
        print(f"❌ 오류 발생: {e}")
