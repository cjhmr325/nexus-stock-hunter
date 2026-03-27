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


# [4] 메인 업데이트 로직 (H열 시작 및 AD열 종료 고정)
def run_update(raw_sheet):
    s_df = yf.download("^NDX", period="5d", interval="1d", auto_adjust=True)
    f_h_df = yf.download("NQ=F", period="5d", interval="1h", auto_adjust=True)
    vxn_df = yf.download("^VXN", period="5d", interval="1d", auto_adjust=True)
    
    # H열(8번째 열)의 모든 데이터를 가져옴
    all_values = raw_sheet.get_all_values()
    # 8번째 열(Index 7)이 Date(H열)인 것을 확인
    existing_dates = [row[7] if len(row) > 7 else "" for row in all_values] 
    
    nexus_raw = get_nexus_master_raw("^NDX") 
    today_str = datetime.now().strftime('%Y-%m-%d')

    for date, s_row in s_df.iterrows():
        curr_date = date.strftime('%Y-%m-%d')
        
        # 데이터 정제
        s_o, s_h, s_l, s_c = force_float(s_row['Open']), force_float(s_row['High']), force_float(s_row['Low']), force_float(s_row['Close'])
        s_v = int(force_float(s_row['Volume']))
        vxn = force_float(vxn_df.loc[date]['Close']) if date in vxn_df.index else 0
        
        target_ts = date.replace(hour=16, minute=0)
        f_match = f_h_df[f_h_df.index.tz_localize(None) <= target_ts.replace(tzinfo=None)].tail(1)
        if not f_match.empty:
            f_o, f_h, f_l, f_c = force_float(f_match['Open']), force_float(f_match['High']), force_float(f_match['Low']), force_float(f_match['Close'])
            f_v = int(force_float(f_match['Volume']))
        else: f_o = f_h = f_l = f_c = f_v = 0

        # [H열~T열] 가격 세트 (13개) + [U열~AD열] 원천 데이터 (10개) = 총 23개
        row_price = [curr_date, round(s_o, 2), round(s_h, 2), round(s_l, 2), round(s_c, 2), s_v, round(f_o, 2), round(f_h, 2), round(f_l, 2), round(f_c, 2), f_v, round(f_c - s_c, 2), round(vxn, 2)]
        row_nexus = nexus_raw if (curr_date == today_str) else [0] * 10
        final_row = row_price + row_nexus # 총 23개 항목

        if curr_date in existing_dates:
            # H열(8번째)부터 AD열(30번째)까지 정확히 타격
            idx = existing_dates.index(curr_date) + 1
            raw_sheet.update(f'H{idx}:AD{idx}', [final_row])
            print(f"✅ {curr_date} 행 업데이트 (H{idx})")
        else:
            # 날짜가 없으면 가장 아래 빈 행(최소 61행)에 삽입
            # 이미지 f4d85c 기준 60행 다음은 61행
            new_idx = len([x for x in existing_dates if x.strip()]) + 1
            if new_idx < 61: new_idx = 61
            
            # H열부터 시작하므로 앞의 A~G열(7개)은 None으로 채움
            raw_sheet.update(f'H{new_idx}:AD{new_idx}', [final_row])
            print(f"✅ {curr_date} 신규 삽입 (H{new_idx})")
            # 중복 방지용 리스트 갱신
            if len(existing_dates) < new_idx:
                existing_dates.extend([""] * (new_idx - len(existing_dates)))
            existing_dates[new_idx-1] = curr_date

if __name__ == "__main__":
    URL = "https://docs.google.com/spreadsheets/d/13oY7i3IWz8npmWsbqC9h9DYJPjAR2XN5XO3R8jlIurk/edit"
    try:
        sheet = connect_to_sheet(URL)
        run_update(sheet)
        print("✅ 밀림 현상 수정 완료 및 데이터 로드 성공")
    except Exception as e: print(f"❌ 오류: {e}")
