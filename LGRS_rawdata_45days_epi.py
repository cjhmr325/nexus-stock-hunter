import os
import json
import gspread
import yfinance as yf
import numpy as np
import pandas as pd
import time
from datetime import datetime
from google.oauth2.service_account import Credentials

# --- [1. 전용 함수: 옵션 지표 계산] ---
# def calculate_option_metrics(ticker_symbol):
#    ... (내용 생략) ...
#    return metrics["T"] + ... + metrics["M"]

# --- [1. 전용 함수: 옵션 지표 계산] ---
def calculate_option_metrics(ticker_symbol):
    tk = yf.Ticker(ticker_symbol)
    try:
        exps = tk.options
        current_price = tk.history(period='1d')['Close'].iloc[-1]
    except:
        return [0] * 15 # 데이터 로드 실패 시 15개 0 반환

    today = datetime.now()
    # T(중심), D(밀도), W(너비), P(자본PCR), M(Total Mass)
    metrics = {"T": [], "D": [], "W": [], "P": [], "M": []}

    if not exps:
        return [0] * 15

    for period in [3, 5, 10]:
        period_data = []
        for exp in exps:
            dte = (datetime.strptime(exp, '%Y-%m-%d') - today).days + 1
            if 0 <= dte <= period:
                try:
                    chain = tk.option_chain(exp)
                    for df, side in [(chain.calls, 'Call'), (chain.puts, 'Put')]:
                        tmp = df[df['openInterest'] > 0].copy()
                        
                        # [위치 계산] Pos = Strike + Price (Call) / Strike - Price (Put)
                        if side == 'Call':
                            tmp['pos'] = tmp['strike'] + tmp['lastPrice']
                        else:
                            tmp['pos'] = tmp['strike'] - tmp['lastPrice']
                        
                        # [핵심 로직] 실질 자본(Locked Mass) = 가격 * OI * 100
                        tmp['mass'] = tmp['lastPrice'] * tmp['openInterest'] * 100
                        tmp['side'] = side
                        period_data.append(tmp[['pos', 'mass', 'side', 'strike']])
                except: continue

        if period_data:
            df = pd.concat(period_data)
            total_m = df['mass'].sum()
            
            # 1. T (중심): 자본 상위 5개 지점의 가중 평균 (노이즈 제거)
            top5 = df.nlargest(5, 'mass')
            t_price = (top5['pos'] * top5['mass']).sum() / top5['mass'].sum()
            
            # 2. D (밀도): 상위 5개 지점 자본 비중 (%)
            dens = (top5['mass'].sum() / total_m) * 100
            
            # 3. W (너비): 자본 70%가 분포하는 가격 범위 ($)
            df_s = df.groupby('pos')['mass'].sum().reset_index().sort_values('pos')
            df_s['cum_pct'] = (df_s['mass'].cumsum() / total_m) * 100
            w_zone = df_s[(df_s['cum_pct'] >= 15) & (df_s['cum_pct'] <= 85)]
            wdth = w_zone['pos'].max() - w_zone['pos'].min() if not w_zone.empty else 0
            
            # 4. P (자본PCR): Put 자본 / Call 자본
            c_m = df[df['side'] == 'Call']['mass'].sum()
            p_m = df[df['side'] == 'Put']['mass'].sum()
            pcr = p_m / c_m if c_m > 0 else 0

            metrics["T"].append(round(t_price, 2))
            metrics["D"].append(round(dens, 1))
            metrics["W"].append(round(wdth, 1))
            metrics["P"].append(round(pcr, 2))
            metrics["M"].append(int(total_m)) # Total Mass 추가
        else:
            for k in metrics: metrics[k].append(0)

    # B~M열(12개) + N~P열(Mass 3개) 총 15개 리턴
    return metrics["T"] + metrics["D"] + metrics["W"] + metrics["P"] + metrics["M"]

# --- [인증 로직 통합본] ---
def get_gspread_client():
    # 1. GitHub Actions 환경 확인 (Secret 변수 존재 여부)
    json_creds = os.environ.get('GOOGLE_SHEETS_JSON')
    
    if json_creds:
        print("🌐 GitHub Actions 환경에서 인증을 시도합니다...")
        creds_dict = json.loads(json_creds)
    else:
        key_filename = 'secret_key.json' 
        
        # 파일이 실제로 존재하는지 체크 (경로 오류 방지)
        if not os.path.exists(key_filename):
            print(f"❌ 에러: {key_filename} 파일이 폴더에 없습니다.")
            print("💡 해결책: 다운로드한 JSON 파일의 이름을 'secret_key.json'으로 바꾸고 파이썬 파일과 같은 폴더에 넣으세요.")
            raise FileNotFoundError(f"{key_filename} missing.")
            
        print(f"💻 로컬 환경({key_filename})에서 인증을 시도합니다...")
        with open(key_filename, 'r', encoding='utf-8') as f:
            creds_dict = json.load(f)

    # Google 서비스 계정 권한 설정
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    return gspread.authorize(creds)

# [실제 실행부]
try:
    gc = get_gspread_client()
    sh = gc.open("dashboard_v2")
    print("✅ 시트 연결 성공! 사냥을 시작합니다.")
except Exception as e:
    print(f"🚨 연결 실패: {e}")
    print("💡 팁: 시트 공유 설정에서 서비스 계정 이메일을 '편집자'로 추가했는지 확인하세요.")
    raise

# 1. 대상 시트 그룹 설정
index_sheets = ["Liquidity", "Gravity", "Resistance", "Stress", "IEnergy", "M_NESM"]
raw_sheets = ["DB_Raw_Price", "DB_Raw_MarketCap", "DB_Raw_Vol", "DB_Raw_High", "DB_Raw_Low", "DB_Raw_PriceOpen", "DB_Raw_Closeyest"]
calc_sheets = ["EPI_History", "Resist_History", "Vector_History","Pressure_History"]
all_ws_names = index_sheets + raw_sheets + calc_sheets
sheets = {}
for name in all_ws_names:
    try:
        sheets[name] = sh.worksheet(name)
        time.sleep(0.2)
    except:
        print(f"⚠️ {name} 시트 누락")


# --- [날짜 동기화 로직] ---
def sync_sheet_dates(target_sheets, date_index):
    formatted_dates = [d.strftime('%Y-%m-%d') for d in reversed(date_index)]
    for name, ws in target_sheets.items():
        # [수정] M_NESM 시트는 헤더를 건드리지 않도록 제외
        if name == "M_NESM":
            continue
        # [교정] raw_sheets 리스트에 있는 시트명인 경우 65일치 날짜를 뿌림
        if name in ["DB_Raw_Price", "DB_Raw_MarketCap", "DB_Raw_Vol", "DB_Raw_High", "DB_Raw_Low", "DB_Raw_PriceOpen", "DB_Raw_Closeyest"]:
            limit = 65
        elif name in index_sheets:
            limit = 250
        else:
            limit = 45 # History 시트들
        date_payload = formatted_dates[:limit]
        end_col_a1 = gspread.utils.rowcol_to_a1(1, limit + 1)
        ws.update([date_payload], f'B1:{end_col_a1}')
        time.sleep(1.2)
    print(f"📅 총 {len(target_sheets)}개 시트 날짜 동기화 완료")

# --- [데이터 수집 및 계산 시작] ---
ws_config = sh.worksheet("Config_Settings")
tickers = [t.strip() for t in ws_config.col_values(1)[1:] if t.strip()]
payloads = {name: [] for name in all_ws_names}
option_final_payload = [] # 옵션 전용 바구니
master_date_index = None
target_today = datetime.now()
global_master_dates = None
print(f"🚀 분석 시작 시각: {target_today.strftime('%Y-%m-%d %H:%M:%S')}")

for ticker in tickers:
    try:
        # 1. 데이터 다운로드
        df = yf.download(ticker, period="2y", interval="1d", progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # [핵심 변경] 가장 최신 날짜를 가진 종목을 기준으로 master_date_index를 계속 갱신
        # 이렇게 하면 어떤 종목은 업데이트가 늦더라도, 업데이트가 빠른 종목을 따라 날짜가 최신화됨
        current_ticker_dates = df.index
        if global_master_dates is None or current_ticker_dates[-1] > global_master_dates[-1]:
            global_master_dates = current_ticker_dates
            print(f"📅 기준 날짜 갱신 ({ticker}): {global_master_dates[-1].strftime('%Y-%m-%d')}")
                

        if master_date_index is None: master_date_index = df.index

        # [메인 루프 내부 - 180라인 근처]
        # opt_row = calculate_option_metrics(ticker)  # <--- 주석 처리
        # option_final_payload.append(opt_row)        # <--- 주석 처리

        # 3. 엑셀 수식용 기초 데이터 추출
        close = df['Close']
        high, low, opens, vol = df['High'], df['Low'], df['Open'], df['Volume']
        close_yest = close.shift(1)

        # 1. 옵션 데이터 초기화
        tk_obj = yf.Ticker(ticker)
        shares = tk_obj.info.get('sharesOutstanding', 0)

        if df.empty or len(df) < 50:
            for name in all_ws_names:
                if name == "Callputoption": continue
                if name == "M_NESM":
                    payloads[name].append([0] * 6) # 6개 추가
                else:
                    limit = 250 if name in index_sheets else 45
                    payloads[name].append([0] * limit)
            continue


        # [B열~G열] 테스트 코드에서 검증된 역순 head(45) 방식
        rev_close = close.iloc[::-1]
        rev_close_yest = close.shift(1).iloc[::-1]

        limit_days = 65 # 변수로 관리하는 것이 좋습니다.
        payloads["DB_Raw_Price"].append(rev_close.head(limit_days).tolist())
        payloads["DB_Raw_Closeyest"].append(rev_close_yest.head(limit_days).fillna(0).tolist())
        payloads["DB_Raw_PriceOpen"].append(opens.iloc[::-1].head(limit_days).tolist())
        payloads["DB_Raw_High"].append(high.iloc[::-1].head(limit_days).tolist())
        payloads["DB_Raw_Low"].append(low.iloc[::-1].head(limit_days).tolist())
        payloads["DB_Raw_Vol"].append(vol.iloc[::-1].head(limit_days).tolist())
        # [마켓캡] - 지표 계산을 위해 원본 Series를 변수로 먼저 잡습니다.
        mcap_series = (close * (shares if shares > 0 else 0))
        # [마켓캡] 예외처리 포함
        mcap_vals = (close * (shares if shares > 0 else 0)).iloc[::-1].head(limit_days).fillna(0).tolist()
        payloads["DB_Raw_MarketCap"].append(mcap_vals)


        # --- [A. 4대 지표 로직 계산] ---
        val = close * vol
        diff = close.diff().fillna(0)

        # 1) Liquidity (유동성 에너지): 대금(val) * 수익률(diff / 어제가격)
        # df_price.shift(1)은 엑셀의 C2(Yesterday Price)와 동일함
        liq_energy = val * (diff / close.shift(1))
        # 무한대(inf)나 결측치(NaN) 처리 후 리스트화 (엑셀의 IFERROR 역할)
        liq_list = liq_energy.replace([np.inf, -np.inf], 0).fillna(0).iloc[::-1].tolist()

        # 2) Gravity (중력 저항): (현재가 / 21일 장중고점) - 1
        # 엑셀: DB_Raw_Price!B2 / MAX(DB_Raw_High!B2:V2) - 1
        max_high_21 = high.rolling(window=21).max()
        grav_list = ((close / max_high_21) - 1).replace([np.inf, -np.inf], 0).fillna(0).iloc[::-1].tolist()

        # 3) Resistance (상대적 위치): (현재 - 20일저점) / (20일고점 - 20일저점)
        # 엑셀: (Price - MIN(Low)) / (MAX(High) - MIN(Low))
        r_max = high.rolling(window=20).max()
        r_min = low.rolling(window=20).min()
        res_range = (r_max - r_min).replace(0, np.nan)
        res_list = ((close - r_min) / res_range).replace([np.inf, -np.inf], 0).fillna(0).iloc[::-1].tolist()

        # 4) Stress (시스템 민감도) - [수정 지점]
        ret_abs = (diff.abs() / close.shift(1))
        # mkt_cap 대신 위에서 만든 mcap_series를 사용 (전체 기간 매칭)
        turnover_ratio = val / mcap_series.replace(0, np.nan) 
        raw_stress = ret_abs / turnover_ratio
        strss_list = raw_stress.replace([np.inf, -np.inf], 0).fillna(0).iloc[::-1].tolist()


        # --- [IEnergy 수식 기반 계산] ---
        # 엑셀 수식: (Price * Vol) * (1 - ABS(변동률)) * SIGN(변동)
        
        # 1. 변동률 및 방향성 계산
        price_diff = close.diff() # 현재가 - 전일가
        ret_pct = price_diff / close.shift(1) # (B2-C2)/C2
        direction = np.sign(price_diff) # SIGN(B2-C2)
        
        # 2. 메인 에너지 계산
        # (Price * Vol) -> 거래대금
        raw_energy = (close * vol) * (1 - ret_pct.abs()) * direction
        
        # 3. 데이터 정제 및 역순 리스트화 (250일 기준)
        ie_list = raw_energy.replace([np.inf, -np.inf], 0).fillna(0).iloc[::-1].tolist()

        # 지표 데이터 할당 (250일 기준)
        index_data = {
            "Liquidity": liq_list,
            "Gravity": grav_list,
            "Resistance": res_list,
            "Stress": strss_list,
            "IEnergy": ie_list
        }
        for name in index_sheets:
            if name == "M_NESM": 
                continue
            # 시트 설정에 맞는 limit 가져오기 (Liquidity 등은 250일)
            d_limit = 250 
            d = index_data[name][:d_limit]
            # [검증] 부족한 데이터는 0으로 채워 정확히 250개를 맞춤
            payloads[name].append(d + [0]*(d_limit - len(d)))
        # --- [EPI & 히스토리 45일 전체 계산 루프] ---
        # 상장주식수 안전 장치
        e2_shares = shares if shares > 0 else 1

        epi_history_45 = []
        resist_history_45 = []
        vector_history_45 = []
        pressure_history_45 = []

        for i in range(45):
            try:
                # 과거 시점 슬라이싱 (i=0이면 오늘까지의 전체 데이터)
                target_df = df if i == 0 else df.iloc[:-i]

                if len(target_df) < 50: # 최소 데이터 확보 확인
                    raise ValueError("Insufficient data")

                # 1. 해당 시점의 기준 데이터 추출
                curr = target_df.iloc[-1]

                # [변수 원복] 시트 수식과 1:1 매칭되는 변수명
                b2_close = curr['Close']
                b2_open = curr['Open']
                b2_high = curr['High']
                b2_low = curr['Low']
                b2_vol = curr['Volume']

                # 1. [신규 추가] Pressure 로직 (전조 증상 지표)
                avg_vol_45 = target_df['Volume'].iloc[-45:].mean()
                rel_vol = b2_vol / avg_vol_45 if avg_vol_45 != 0 else 0

                denom = (b2_high - b2_low) + 0.001
                up_force = (b2_close - b2_low) / denom
                down_force = (b2_high - b2_close) / denom

                # [사용자 제공 EPI 수식 그대로 구현]
                # Relative_Vol: 당일 거래량 / 45일 평균 거래량
                avg_vol_45 = target_df['Volume'].iloc[-45:].mean()
                relative_vol = b2_vol / avg_vol_45 if avg_vol_45 != 0 else 0

                # Up_Force, Down_Force
                denom = (b2_high - b2_low) + 0.001
                up_force = (b2_close - b2_low) / denom
                down_force = (b2_high - b2_close) / denom

                # Pressure & Net_Thrust
                pressure = up_force + down_force  # 결과적으로 거의 1
                net_thrust = relative_vol * (up_force - down_force)

                # 최종 EPI 값 (AM열에 들어가는 값)
                epi_val = net_thrust * pressure

                # 2. [기본 로직 원복] 이동평균 및 표준편차
                r20_prices = target_df['Close'].iloc[-20:]
                r5_prices = target_df['Close'].iloc[-5:]
                avg20 = r20_prices.mean()
                avg5 = r5_prices.mean()
                std20 = r20_prices.std(ddof=1)
                std5 = r5_prices.std(ddof=1)
                std_ratio = std5 / std20 if std20 != 0 else 0

                # 3. [기본 로직 원복] 추진력 및 저항 인자
                s2 = (b2_close / b2_open) - 1
                t2 = ((b2_close / avg5) - 1) * std_ratio
                u2 = ((b2_close / avg20) - 1) * std_ratio

                v_eff_deno = b2_vol / e2_shares
                v_efficiency = s2 / v_eff_deno if v_eff_deno != 0 else 0
                weighted_thrust = (s2 * 0.5) + (t2 * 0.3) + (u2 * 0.2)


                # 4. 개별 저항 인자 (X, Y, Z, AA, AB)
                x2_drag = (s2 ** 2) * std5
                y2_elastic = abs((b2_close / avg20) - 1)
                z2_friction = v_eff_deno

                mcap_now = b2_close * e2_shares
                aa2_inertia = np.log10(mcap_now) if mcap_now > 0 else 0

                high_45_avg = target_df['High'].iloc[-45:].mean()
                low_45_avg = target_df['Low'].iloc[-45:].mean()
                ab2_amplitude = (high_45_avg - low_45_avg) / (b2_high - b2_low + 0.001)

                # 4. [기본 로직 원복] AC 통합 저항 및 AN 최종 벡터
                combined_factor = (1 + y2_elastic) * (1 + z2_friction) * (1 + x2_drag)
                ac_input = 1 + (combined_factor * ab2_amplitude * (aa2_inertia / 10))
                ac_total = np.log10(ac_input) if ac_input > 0 else 0

                # 6. AN 최종 가속 벡터 산출 (Nexus Logic 적용: 분모 1+AC)
                weighted_thrust = (s2 * 0.5) + (t2 * 0.3) + (u2 * 0.2)
                final_an = (weighted_thrust * v_efficiency) / (1 + ac_total)

                # 결과값 리스트에 순차적으로 추가 (B열부터 AS열 방향)
                epi_history_45.append(round(epi_val, 12))        # EPI 시트용
                resist_history_45.append(round(ac_total, 10))    # Resist 시트용
                vector_history_45.append(round(final_an, 12))     # Vector 시트용
                pressure_history_45.append(round(net_thrust, 12)) # Pressure 시트용

            except Exception as e:
                epi_history_45.append(0); resist_history_45.append(0)
                vector_history_45.append(0); pressure_history_45.append(0)

        # 7. 페이로드 할당 (각 시트 이름에 맞춰 전송 데이터 구성)
        payloads["EPI_History"].append(epi_history_45)
        payloads["Resist_History"].append(resist_history_45)
        payloads["Vector_History"].append(vector_history_45)
        payloads["Pressure_History"].append(pressure_history_45)


        # --- [M_NESM: 누적 거래량 가중 방향성 에너지] ---
        denom_shares = shares if shares > 0 else 1 

        # 1. 일별 방향성 * (거래량 / 총주식수) 계산
        # 데이터가 과거->오늘 순이므로 diff()는 (오늘-어제) 방향이 됨
        price_direction = np.sign(close.diff().fillna(0))
        vol_weight = vol / denom_shares
        daily_energy = price_direction * vol_weight

        # 2. 기간별 MAX SCAN 값 산출
        nesm_row = []
        nesm_periods = [60, 80, 100, 120, 140, 160]

        for p in nesm_periods:
            if len(daily_energy) >= p:
                # [CRITICAL MERGE POINT]
                # 1. tail(p): 최근 p일치를 가져온다.
                # 2. iloc[::-1]: 오늘이 맨 위로 오게 뒤집는다. (엑셀 B2가 시작점이 되도록)
                # 3. cumsum(): 오늘부터 과거로 가며 누적합을 구한다.
                # 4. max(): 그중 가장 컸던 고점(Peak)을 찾는다.
                scan_values = daily_energy.tail(p).iloc[::-1].cumsum()
                nesm_val = scan_values.max()
            else:
                nesm_val = 0
            nesm_row.append(round(nesm_val, 8))

        # M_NESM 전용 페이로드에 저장
        payloads["M_NESM"].append(nesm_row)


        print(f"✅ {ticker} 분석 완료")
        time.sleep(0.05)



    except Exception as e:
        print(f"❌ {ticker} 상세 오류: {type(e).__name__} - {e}")
        for name in all_ws_names:
            if name == "Callputoption": continue
            
            # [수정] M_NESM인 경우 6개, 나머지는 기존대로 처리
            if name == "M_NESM":
                payloads[name].append([0] * 6)
            else:
                limit = 250 if name in index_sheets else 45
                payloads[name].append([0] * limit)

# 3. 날짜 및 데이터 일괄 업데이트
if master_date_index is not None:
    sync_sheet_dates(sheets, master_date_index)

# 마지막 업데이트 루프 부분 보완
# [최종 보완된 업데이트 루프]
for name in all_ws_names:
    if name not in sheets: continue
    ws = sheets[name]
    data = payloads[name]
    if name == "M_NESM":
        # M_NESM은 BA열(53번째 열)부터가 아니라, 보통 A열 티커 뒤인 B열부터 6개를 넣는 것이 관리상 편합니다.
        # 질문하신 "BA부터"를 적용하려면 아래와 같이 범위를 지정합니다.
        ws.batch_clear(["A2:A1000", "BA2:BF1000"]) 
        ws.update([[t] for t in tickers], f'A2:A{len(tickers)+1}')
        ws.update(data, f'BA2:BF{len(tickers)+1}')
    else:
        ticker_count = len(tickers)

        # 1. 시트 유형별 동적 한계치(limit) 설정
        if name in raw_sheets:
            current_limit = 65
            end_letter = "BZ"  # 65열(BN)보다 넉넉하게 BZ까지 청소
        elif name in index_sheets:
            current_limit = 250
            end_letter = "IP"  # 250열(IP)까지 청소
        else:
            current_limit = 45  # 히스토리/계산 시트용
            end_letter = "AZ"

        # 2. 기존 데이터 및 유령 데이터 청소
        ws.batch_clear([f"A2:{end_letter}1000"])
        time.sleep(0.5)

        if ticker_count > 0:
            # 3. 티커 리스트 업데이트 (A열)
            ws.update([[t] for t in tickers], f'A2:A{ticker_count + 1}')

            # 4. 계산된 데이터 업데이트 (B열부터 설정된 current_limit까지)
            # [교정]: 위에서 정한 current_limit을 그대로 사용해야 65일치가 들어갑니다.
            end_col_a1 = gspread.utils.rowcol_to_a1(ticker_count + 1, current_limit + 1)
            ws.update(data, f'B2:{end_col_a1}')

    print(f"✨ {name} 시트: {ticker_count}개 종목 업데이트 완료 (범위: {current_limit}일)")
    time.sleep(1) # API Quota 방어

# [최종 업데이트부 - 365라인 근처]
# try:
#     ws_opt = sh.worksheet("Callputoption")
#     ws_opt.batch_clear(["B2:P500"])  # <--- 주석 처리
#     time.sleep(0.5)
#     if option_final_payload:
#         ws_opt.update(option_final_payload, "B2")  # <--- 주석 처리
# except Exception as e:
#     print(f"🚨 업데이트 중 오류 발생: {e}")
