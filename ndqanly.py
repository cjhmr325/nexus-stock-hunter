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

# [3] 원천 데이터 추출 (기존 10개 + 신규 10개 = 총 20개 리턴)
def get_nexus_master_raw(ticker_symbol):
    tk = yf.Ticker(ticker_symbol)
    try:
        exps = tk.options
        if not exps: return [0]*20
        chain = None
        target_date = exps[0]
        for exp in exps[:2]:
            tmp_chain = tk.option_chain(exp)
            if not tmp_chain.calls.empty and tmp_chain.calls['openInterest'].sum() > 0:
                chain = tmp_chain
                target_date = exp
                break
        if chain is None: return [0]*20
        
        calls, puts = chain.calls, chain.puts
        c_active = calls[calls['openInterest'] > 0].copy()
        p_active = puts[puts['openInterest'] > 0].copy()

        # 기존 통계 데이터 (U~AD)
        c_oi_m = (c_active['strike'] * c_active['openInterest'] * 100).sum()
        p_oi_m = (p_active['strike'] * p_active['openInterest'] * 100).sum()
        c_vol_m = (c_active['strike'] * c_active['volume'].fillna(0) * 100).sum()
        p_vol_m = (p_active['strike'] * p_active['volume'].fillna(0) * 100).sum()
        c_avg_s = (c_active['strike'] * c_active['openInterest']).sum() / c_active['openInterest'].sum()
        p_avg_s = (p_active['strike'] * p_active['openInterest']).sum() / p_active['openInterest'].sum()
        avg_iv = (c_active['impliedVolatility'].mean() + p_active['impliedVolatility'].mean()) / 2
        top5_sum = c_active.nlargest(5, 'openInterest')['openInterest'].sum() + p_active.nlargest(5, 'openInterest')['openInterest'].sum()
        
        # [신규] 콜/풋 각각 상위 5개 Strike 추출 (AE~AN)
        c_top5_strikes = c_active.nlargest(5, 'openInterest')['strike'].tolist()
        p_top5_strikes = p_active.nlargest(5, 'openInterest')['strike'].tolist()
        
        # 5개 미만일 경우 0으로 채움
        while len(c_top5_strikes) < 5: c_top5_strikes.append(0)
        while len(p_top5_strikes) < 5: p_top5_strikes.append(0)

        # Max Pain 및 DTE (원본 로직)
        strikes = pd.concat([c_active['strike'], p_active['strike']]).unique()
        strikes = np.sort(strikes)[::5]
        def get_pain(s):
            return c_active[c_active['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum() + \
                   p_active[p_active['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
        pains = [get_pain(s) for s in strikes]
        max_pain = strikes[np.argmin(pains)] if len(pains) > 0 else 0
        dte = (datetime.strptime(target_date, '%Y-%m-%d') - datetime.now()).days

        # 기존 10개 + 콜 5개 + 풋 5개 = 총 20개 리턴
        return [int(c_oi_m), int(p_oi_m), int(c_vol_m), int(p_vol_m), round(float(max_pain), 2), int(top5_sum), 
                round(float(c_avg_s), 2), round(float(p_avg_s), 2), round(float(avg_iv), 4), int(dte)] + \
                c_top5_strikes + p_top5_strikes
    except: return [0]*20


# [4] 메인 업데이트 로직 (원본 보존 + 범위 확장)
def run_update(raw_sheet):
    s_df = yf.download("^NDX", period="5d", interval="1d", auto_adjust=True)
    f_h_df = yf.download("NQ=F", period="5d", interval="1d", auto_adjust=True)
    vxn_df = yf.download("^VXN", period="5d", interval="1d", auto_adjust=True)
    
    all_values = raw_sheet.get_all_values()
    existing_dates = [row[7] if len(row) > 7 else "" for row in all_values] 
    
    if s_df.empty: return
    today_str = s_df.index[-1].strftime('%Y-%m-%d')

    nexus_raw_today = get_nexus_master_raw("^NDX") 
    

    for date, s_row in s_df.iterrows():
        curr_date = date.strftime('%Y-%m-%d')
        
        # 1. 가격 세트 (H~T)
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

        # 2. 옵션 세트 결정 (U~AN) - 총 20개 항목
        row_nexus = [0] * 20 
        
        if curr_date in existing_dates:
            idx = existing_dates.index(curr_date)
            current_row_data = all_values[idx] if idx < len(all_values) else []
            
            if curr_date == today_str:
                row_nexus = nexus_raw_today
            elif len(current_row_data) > 20:
                # U열(20)부터 AN열(39)까지 20개 추출
                existing_nexus = current_row_data[20:40]
                if any(str(val).strip() not in ["0", "0.0", ""] for val in existing_nexus):
                    row_nexus = (existing_nexus + [0]*20)[:20]
                else:
                    row_nexus = [0] * 20
        else:
            row_nexus = nexus_raw_today if (curr_date == today_str) else [0] * 20

        # 3. 최종 결합 및 업데이트 (H ~ AN)
        final_row = row_price + row_nexus

        if curr_date in existing_dates:
            row_num = existing_dates.index(curr_date) + 1
            # 범위를 AN까지 확장
            raw_sheet.update(f'H{row_num}:AN{row_num}', [final_row], value_input_option='USER_ENTERED')
            print(f"✅ {curr_date} 업데이트 완료")
        else:
            new_idx = len([x for x in existing_dates if x.strip()]) + 1
            if new_idx < 61: new_idx = 61
            raw_sheet.update(f'H{new_idx}:AN{new_idx}', [final_row], value_input_option='USER_ENTERED')
            print(f"✅ {curr_date} 신규 삽입 (H{new_idx})")

if __name__ == "__main__":
    URL = "https://docs.google.com/spreadsheets/d/13oY7i3IWz8npmWsbqC9h9DYJPjAR2XN5XO3R8jlIurk/edit"
    try:
        sheet = connect_to_sheet(URL)
        run_update(sheet)
        print("🚀 모든 작업 성공")
    except Exception as e: 
        print(f"❌ 오류: {e}")
