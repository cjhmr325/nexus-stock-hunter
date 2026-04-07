import yfinance as yf
import pandas as pd
import numpy as np
import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import re
import json  # 이 줄이 반드시 있어야 json.loads()를 사용할 수 있습니다.
import os
import yfinance as yf


# --- [기본 설정] ---
CSV_FILE = 'nexus_accumulated.csv'
SHEET_URL = "https://docs.google.com/spreadsheets/d/13oY7i3IWz8npmWsbqC9h9DYJPjAR2XN5XO3R8jlIurk/edit"
SECRET_JSON = 'secret_key.json'
SLIDING_SHEET = "Callputoption"

def get_google_client():
    # 1. GitHub Actions 환경 변수 확인
    json_creds = os.environ.get('GOOGLE_SHEETS_JSON')
    
    if json_creds:
        # GitHub 서버: Secrets에서 가져온 JSON 문자열 사용
        creds_dict = json.loads(json_creds)
    else:
        # 로컬 환경: 폴더 내 secret_key.json 파일 사용
        key_filename = 'secret_key.json' 
        if not os.path.exists(key_filename):
            raise FileNotFoundError(f"❌ {key_filename} 파일이 로컬에 없습니다.")
        with open(key_filename, 'r', encoding='utf-8') as f:
            creds_dict = json.load(f)

    # 권한 범위 설정
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(creds)

def get_nexus_snapshot_final(ticker):    
    """[Wide-Spectrum Engine] 8개 만기일 통합 및 연산 최적화 버전"""
    try:
        tk = yf.Ticker(ticker)
        # 데이터 연속성 확인을 위해 7일치 수집
        hist = tk.history(period="7d")
        if hist.empty or len(hist) < 2: return None
        
        actual_market_date = hist.index[-1].strftime('%Y-%m-%d')
        update_at = datetime.now().strftime('%H:%M:%S')
        
        cp, pc = hist['Close'].iloc[-1], hist['Close'].iloc[-2]
        exps = tk.options
        if not exps: return None
        
        # --- [데이터 수집: 8개 만기일 확장] ---
        all_c, all_p = [], []
        # 만기일이 8개 미만일 경우를 대비해 min() 처리
        target_exps = exps[:min(8, len(exps))] 
        
        for exp in target_exps:
            chain = tk.option_chain(exp)
            if not chain.calls.empty: all_c.append(chain.calls)
            if not chain.puts.empty: all_p.append(chain.puts)
        
        if not all_c or not all_p: return None
        c_df, p_df = pd.concat(all_c), pd.concat(all_p)
        
        # --- [연산 섹션: Notional Value] ---
        c_oi_m = (c_df['strike'] * c_df['openInterest'] * 100).sum()
        p_oi_m = (p_df['strike'] * p_df['openInterest'] * 100).sum()
        c_vol_m = (c_df['strike'] * c_df['volume'].fillna(0) * 100).sum()
        p_vol_m = (p_df['strike'] * p_df['volume'].fillna(0) * 100).sum()
        
        # 행사가별 그룹화 (중복 제거 및 합산)
        c_agg = c_df.groupby('strike').agg({'openInterest':'sum', 'volume':'sum', 'impliedVolatility':'mean'}).reset_index()
        p_agg = p_df.groupby('strike').agg({'openInterest':'sum', 'volume':'sum', 'impliedVolatility':'mean'}).reset_index()
        
        # --- [Max Pain 최적화 연산] ---
        # 8개 만기일 합산 시 strikes가 매우 많아지므로 현재가 기준 ±30% 범위로 제한하여 속도 개선
        strikes = np.sort(pd.concat([c_agg['strike'], p_agg['strike']]).unique())
        scope_strikes = strikes[(strikes > cp * 0.7) & (strikes < cp * 1.3)]
        
        pains = []
        for s in scope_strikes:
            c_loss = c_agg[c_agg['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum()
            p_loss = p_agg[p_agg['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
            pains.append(c_loss + p_loss)
            
        max_pain = float(scope_strikes[np.argmin(pains)]) if len(pains) > 0 else float(cp)
        avg_iv = float((c_agg['impliedVolatility'].mean() + p_agg['impliedVolatility'].mean()) / 2)
        net_force = (c_vol_m - p_vol_m) / (c_vol_m + p_vol_m + 1)
        
        # --- [결과 셋업] ---
        res = {
            'Market_Date': actual_market_date, 'Ticker': ticker, 'Price': round(cp, 2),
            'G_Energy': ((max_pain - cp) / cp) / (avg_iv + 0.01) ,
            'Net_Force': net_force, 'Doubt_Zone': 1 if (cp < pc and net_force > 0.2) else 0,
            'Update_At': update_at, 'Prev_Close': round(pc, 2),
            'Stock_Vol': int(hist['Volume'].iloc[-1]),
            'Call_OI_M': int(c_oi_m), 'Put_OI_M': int(p_oi_m),
            'Max_Pain': max_pain, 'Avg_IV': round(avg_iv, 4),
            'PCR_OI': round(p_oi_m / (c_oi_m + 1), 3)
        }

        # [T1~T5 상세 데이터 복구]
        c_top = c_agg.nlargest(5, 'openInterest')
        p_top = p_agg.nlargest(5, 'openInterest')
        for i in range(5):
            res[f'C_T{i+1}_S'] = float(c_top.iloc[i]['strike']) if i < len(c_top) else 0.0
            res[f'C_T{i+1}_V'] = int(c_top.iloc[i]['volume']) if i < len(c_top) else 0
            res[f'P_T{i+1}_S'] = float(p_top.iloc[i]['strike']) if i < len(p_top) else 0.0
            res[f'P_T{i+1}_V'] = int(p_top.iloc[i]['volume']) if i < len(p_top) else 0
        
        # 1. 거리 및 방향성 계산 (절대값 제거)
        # raw_dist > 0 : 주가 위에 벽이 있음 (저항)
        # raw_dist < 0 : 주가 아래에 벽이 있음 (지지)
        raw_dist = res['C_T1_S'] - res['Price']

        # 2. 거래량 비중 계산
        total_c_vol = c_df['volume'].sum() + 1 # ZeroDivision 방지
        c_t1_ratio = res['C_T1_V'] / total_c_vol

        # 3. Strike_Impact 재연산 (매직넘버 100 제거)
        # 비선형 거리 감쇄 로직 적용
        impact_magnitude = c_t1_ratio * (1 / (np.sqrt(abs(raw_dist)) + 1))

        # 4. 방향성(Sign) 부여 및 최종 저장
        # 양수(+)면 저항(천장), 음수(-)면 지지(바닥)
        res['Strike_Impact'] = impact_magnitude * np.sign(raw_dist)
        
        return res
    except Exception as e:
        print(f"⚠️ {ticker} Wide-Scan Error: {e}")
        return None

def run_sliding_push_final(doc, df_latest):
    """[Overwrite/Insert 분기] B열 기점 스마트 업데이트"""
    ws = doc.worksheet(SLIDING_SHEET)
    latest_date = str(df_latest['Market_Date'].iloc[0])
    
    # 1. 기준점 체크 (M열은 13번째 열입니다)
    # M1 셀의 값을 가져와 날짜 비교
    m1_val = ws.cell(1, 13).value or "" 
    match = re.search(r'\d{4}-\d{2}-\d{2}', m1_val)
    sheet_latest_date = match.group(0) if match else None

    # 2. 날짜 다를 경우 M열 기점으로 7칸 삽입 (Insert)
    if sheet_latest_date != latest_date:
        print(f"📅 날짜 변경({sheet_latest_date} -> {latest_date}): M열에 7개 슬롯 삽입.")
        requests = [{
            "insertDimension": {
                "range": {
                    "sheetId": ws._properties['sheetId'],
                    "dimension": "COLUMNS",
                    "startIndex": 12, # M열(Index 12)부터 시작
                    "endIndex": 19    # S열(Index 19)까지 총 7칸
                },
                "inheritFromBefore": True
            }
        }]
        doc.batch_update({"requests": requests})
        
        # M1~S1 헤더 7개 동시 생성
        headers = [[
            f"{latest_date}_G_Energy", f"{latest_date}_Net_Force", f"{latest_date}_Doubt_Zone", 
            f"{latest_date}_Strike_Impact", f"{latest_date}_C_T1_S", f"{latest_date}_P_T1_S", f"{latest_date}_Total_Mass"
        ]]
        ws.update("M1:S1", headers)
    else:
        print(f"🔄 동일 날짜: M~S열 데이터 갱신(Overwrite).")

    # 3. 데이터 매핑 및 전송 (M~S열)
    tickers_in_sheet = ws.col_values(1)[1:]
    data_map = df_latest.set_index('Ticker').to_dict('index')
    
    update_matrix = []
    for t in tickers_in_sheet:
        row = data_map.get(t, {})
        if row:
            # 7개 지표 순서대로 정렬
            update_matrix.append([
                row.get('G_Energy', '-'), 
                row.get('Net_Force', '-'), 
                row.get('Doubt_Zone', '-'), 
                row.get('Strike_Impact', '-'),
                row.get('C_T1_S', 0),
                row.get('P_T1_S', 0),
                row.get('Call_OI_M', 0) # Total Mass 대용
            ])
        else:
            update_matrix.append(['-'] * 7)

    # 4. M2:S{끝} 범위에 최종 업데이트
    if update_matrix:
        range_label = f"M2:S{len(tickers_in_sheet) + 1}"
        ws.update(range_label, update_matrix)
        
    print(f"✨ M열 기점 7칸 슬라이딩 업데이트 완료.")

def full_execution():
    """전체 리스트 프로세스 실행"""
    client = get_google_client()
    doc = client.open_by_url(SHEET_URL)
    
    # Config_Settings 시트의 모든 티커 로드
    all_tickers = [t.strip() for t in doc.worksheet("Config_Settings").col_values(1)[1:] if t.strip()]
    
    print(f"🚀 전체 {len(all_tickers)}개 종목 가동...")
    results = []
    for t in all_tickers:
        data = get_nexus_snapshot_final(t)
        if data:
            results.append(data)
            print(f"[{data['Market_Date']}] {t} 수집 성공")
        time.sleep(0.4)

    if not results: return
    df_new = pd.DataFrame(results)
    
    # [1] CSV 누적 (모든 상세 필드 포함)
    if os.path.exists(CSV_FILE):
        old = pd.read_csv(CSV_FILE)
        df_total = pd.concat([old, df_new]).drop_duplicates(subset=['Market_Date', 'Ticker'], keep='last')
    else:
        df_total = df_new
    df_total.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')

    # [2] 분석 시트 업데이트 (B열 기점)
    run_sliding_push_final(doc, df_new)
    print("✨ 전체 리스트 프로세스 완료.")

if __name__ == "__main__":
    full_execution()