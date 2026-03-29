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

# [3] get_nexus_master_raw (친절한 짝꿍 배치 버전)
def get_nexus_master_raw(ticker_symbol):
    tk = yf.Ticker(ticker_symbol)
    try:
        exps = tk.options
        if not exps: return [0]*30
        
        # 1. [확장] 최대 8개의 만기일을 가져와서 하나의 데이터프레임으로 통합
        all_calls = []
        all_puts = []
        
        # 상위 8개 만기일 전수 조사 (약 2달치)
        target_exps = exps[:8] 
        for exp in target_exps:
            chain = tk.option_chain(exp)
            # 만기일 정보를 포함하여 데이터 수집
            c = chain.calls.copy()
            p = chain.puts.copy()
            c['expiry'] = exp
            p['expiry'] = exp
            all_calls.append(c)
            all_puts.append(p)
            
        if not all_calls or not all_puts: return [0]*30
        
        # 전체 데이터 통합
        df_calls = pd.concat(all_calls, ignore_index=True)
        df_puts = pd.concat(all_puts, ignore_index=True)

        # 2. [핵심] 가격대(Strike)별로 모든 만기일의 OI와 Volume을 합산 (Aggregation)
        agg_calls = df_calls.groupby('strike').agg({
            'openInterest': 'sum',
            'volume': 'sum',
            'impliedVolatility': 'mean'
        }).reset_index()
        
        agg_puts = df_puts.groupby('strike').agg({
            'openInterest': 'sum',
            'volume': 'sum',
            'impliedVolatility': 'mean'
        }).reset_index()

        # [기본 통계 10개] - 통합 데이터 기반 연산
        c_oi_m = (agg_calls['strike'] * agg_calls['openInterest'] * 100).sum()
        p_oi_m = (agg_puts['strike'] * agg_puts['openInterest'] * 100).sum()
        c_vol_m = (agg_calls['strike'] * agg_calls['volume'].fillna(0) * 100).sum()
        p_vol_m = (agg_puts['strike'] * agg_puts['volume'].fillna(0) * 100).sum()
        
        c_avg_s = (agg_calls['strike'] * agg_calls['openInterest']).sum() / agg_calls['openInterest'].sum()
        p_avg_s = (agg_puts['strike'] * agg_puts['openInterest']).sum() / agg_puts['openInterest'].sum()
        
        avg_iv = (agg_calls['impliedVolatility'].mean() + agg_puts['impliedVolatility'].mean()) / 2
        
        # 통합 데이터 기준 TOP 5 OI 합계
        top5_sum = agg_calls.nlargest(5, 'openInterest')['openInterest'].sum() + \
                    agg_puts.nlargest(5, 'openInterest')['openInterest'].sum()
        
        # Max Pain 계산 (전체 통합 스트라이크 기준)
        strikes = pd.concat([agg_calls['strike'], agg_puts['strike']]).unique()
        strikes = np.sort(strikes)[::5] # 연산 최적화
        
        def get_pain(s):
            return agg_calls[agg_calls['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum() + \
                   agg_puts[agg_puts['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
        
        pains = [get_pain(s) for s in strikes]
        max_pain = strikes[np.argmin(pains)] if len(pains) > 0 else 0
        
        # DTE는 가장 가까운 만기일 기준 유지
        dte = (datetime.strptime(exps[0], '%Y-%m-%d') - datetime.now()).days

        basic_stats = [int(c_oi_m), int(p_oi_m), int(c_vol_m), int(p_vol_m), round(float(max_pain), 2), 
                       int(top5_sum), round(float(c_avg_s), 2), round(float(p_avg_s), 2), round(float(avg_iv), 4), int(dte)]

        # 3. [지뢰 포착] 날짜와 상관없이 "통합 화력"이 가장 강력한 TOP 5 지점 추출
        c_top5 = agg_calls.nlargest(5, 'openInterest')
        p_top5 = agg_puts.nlargest(5, 'openInterest')
        
        c_pair = []
        for _, row in c_top5.iterrows():
            c_pair.extend([round(float(row['strike']), 2), int(row['openInterest'])])
        
        p_pair = []
        for _, row in p_top5.iterrows():
            p_pair.extend([round(float(row['strike']), 2), int(row['openInterest'])])
        
        # 0 채우기 (데이터 부족 대비)
        while len(c_pair) < 10: c_pair.extend([0, 0])
        while len(p_pair) < 10: p_pair.extend([0, 0])

        return basic_stats + c_pair + p_pair # 총 30개 리턴
    except Exception as e:
        # print(f"Error on {ticker_symbol}: {e}") # 디버깅 필요시 해제
        return [0]*30
    

# [4] 메인 업데이트 로직
def run_update(raw_sheet):
    s_df = yf.download("^NDX", period="5d", interval="1d", auto_adjust=True)
    f_h_df = yf.download("NQ=F", period="5d", interval="1d", auto_adjust=True)
    vxn_df = yf.download("^VXN", period="5d", interval="1d", auto_adjust=True)
    
    if s_df.empty: return
    
    all_values = raw_sheet.get_all_values()
    existing_dates = [row[7] if len(row) > 7 else "" for row in all_values]
    search_dates = [d[:10] for d in existing_dates]

    now = datetime.now()
    current_run_time = now.strftime('%m-%d %H:%M')
    today_str = s_df.index[-1].strftime('%Y-%m-%d')
    nexus_raw_today = get_nexus_master_raw("^NDX")

    for date, s_row in s_df.iterrows():
        curr_date = date.strftime('%Y-%m-%d')
        record_date = f"{curr_date} [{current_run_time}]" if curr_date == today_str else curr_date
        
        # 가격 세트 (H~T: 13개)
        s_c = force_float(s_row['Close'])
        vxn = force_float(vxn_df.loc[date]['Close']) if date in vxn_df.index else 0
        target_ts = date.replace(hour=16, minute=0)
        f_match = f_h_df[f_h_df.index.tz_localize(None) <= target_ts.replace(tzinfo=None)].tail(1)
        f_c = force_float(f_match['Close']) if not f_match.empty else 0
        
        row_price = [record_date, round(force_float(s_row['Open']), 2), round(force_float(s_row['High']), 2), 
                     round(force_float(s_row['Low']), 2), round(s_c, 2), int(force_float(s_row['Volume'])),
                     round(force_float(f_match['Open']), 2) if not f_match.empty else 0,
                     round(force_float(f_match['High']), 2) if not f_match.empty else 0,
                     round(force_float(f_match['Low']), 2) if not f_match.empty else 0,
                     round(f_c, 2), int(force_float(f_match['Volume'])) if not f_match.empty else 0,
                     round(f_c - s_c, 2), round(vxn, 2)]

        # 옵션 세트 (U~AX: 30개)
        row_nexus = [0] * 30
        if curr_date in search_dates:
            idx = search_dates.index(curr_date)
            if curr_date == today_str: row_nexus = nexus_raw_today
            else:
                row_data = all_values[idx] if idx < len(all_values) else []
                if len(row_data) > 20: row_nexus = (row_data[20:50] + [0]*30)[:30]
        else:
            row_nexus = nexus_raw_today if curr_date == today_str else [0]*30

        final_row = row_price + row_nexus
        
        if curr_date in search_dates:
            row_num = search_dates.index(curr_date) + 1
            raw_sheet.update(f'H{row_num}:AX{row_num}', [final_row], value_input_option='USER_ENTERED')
            print(f"✅ {curr_date} 업데이트")
        else:
            new_idx = max(len([x for x in existing_dates if x.strip()]) + 1, 61)
            raw_sheet.update(f'H{new_idx}:AX{new_idx}', [final_row], value_input_option='USER_ENTERED')
            print(f"✅ {curr_date} 신규 삽입")

if __name__ == "__main__":
    URL = "https://docs.google.com/spreadsheets/d/13oY7i3IWz8npmWsbqC9h9DYJPjAR2XN5XO3R8jlIurk/edit"
    try:
        sheet = connect_to_sheet(URL)
        run_update(sheet)
        print("🚀 Alpha Engine 레이더 가동 성공")
    except Exception as e: print(f"❌ 오류: {e}")
