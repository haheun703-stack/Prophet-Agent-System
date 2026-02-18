"""
CSV Data Loader - stock_data_daily CSV 로더
=============================================
stock_data_daily/ 폴더의 일봉 CSV 데이터를 로드하여
백테스트 및 전략 평가에 활용

CSV 컬럼:
  Date, Open, High, Low, Close, Volume,
  MA5, MA20, MA60, MA120, RSI, MACD, MACD_Signal,
  Upper_Band, Lower_Band, ATR, Stoch_K, Stoch_D, OBV,
  Next_Close, Target, MarketCap,
  EMA1, EMA2, EMA3, TRIX, TRIX_Signal,
  Plus_DM, Minus_DM, Plus_DM_14, Minus_DM_14,
  Plus_DI, Minus_DI, DX, ADX,
  Foreign_Net, Inst_Net
"""

import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger('Scalper.CSVLoader')


class CSVLoader:
    """stock_data_daily CSV 데이터 로더"""

    def __init__(self, data_dir: str = None):
        """
        Args:
            data_dir: stock_data_daily 폴더 경로
                      None이면 프로젝트 루트의 stock_data_daily 사용
        """
        if data_dir is None:
            # scalper-agent 기준 상위 폴더의 stock_data_daily
            base = Path(__file__).resolve().parent.parent.parent
            data_dir = str(base / "stock_data_daily")
        self.data_dir = Path(data_dir)
        self._file_map: Optional[Dict[str, Path]] = None

    def _build_file_map(self):
        """파일명에서 종목코드 추출하여 매핑 생성"""
        if self._file_map is not None:
            return
        self._file_map = {}

        if not self.data_dir.exists():
            logger.warning(f"데이터 디렉토리 없음: {self.data_dir}")
            return

        for f in self.data_dir.glob("*.csv"):
            name = f.stem  # e.g. "삼성전자_005930" or "Stock_0010V0"
            # 종목코드 추출 (6자리 숫자)
            match = re.search(r'_(\d{6})$', name)
            if match:
                code = match.group(1)
                self._file_map[code] = f
            else:
                # Stock_ 접두사 파일은 코드 형식이 다름 (무시)
                pass

        logger.info(f"CSV 로더 초기화: {len(self._file_map)}개 종목 발견 "
                     f"({self.data_dir})")

    def get_available_codes(self) -> List[str]:
        """사용 가능한 종목코드 목록"""
        self._build_file_map()
        return sorted(self._file_map.keys())

    def get_code_name(self, code: str) -> str:
        """종목코드 → 종목명"""
        self._build_file_map()
        path = self._file_map.get(code)
        if path:
            name = path.stem  # "삼성전자_005930"
            return name.rsplit('_', 1)[0]
        return code

    def get_code_name_map(self) -> Dict[str, str]:
        """전체 종목코드→종목명 매핑"""
        self._build_file_map()
        return {code: self.get_code_name(code) for code in self._file_map}

    def load(self, code: str,
             start_date: str = None,
             end_date: str = None) -> Optional[pd.DataFrame]:
        """
        종목 일봉 데이터 로드

        Args:
            code: 종목코드 (6자리)
            start_date: 시작일 (YYYY-MM-DD 또는 YYYYMMDD)
            end_date: 종료일

        Returns:
            DataFrame with lowercase columns:
            [date, open, high, low, close, volume, ma5, ma20, ma60, ma120,
             rsi, macd, macd_signal, upper_band, lower_band, atr,
             stoch_k, stoch_d, obv, next_close, target, marketcap,
             foreign_net, inst_net, adx, ...]
        """
        self._build_file_map()

        path = self._file_map.get(code)
        if path is None:
            logger.warning(f"종목 데이터 없음: {code}")
            return None

        try:
            df = pd.read_csv(path, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding='cp949')

        # 컬럼명 소문자 변환
        df.columns = [c.lower().strip() for c in df.columns]

        # 날짜 파싱
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)

            if start_date:
                start_date = pd.to_datetime(start_date)
                df = df[df['date'] >= start_date]
            if end_date:
                end_date = pd.to_datetime(end_date)
                df = df[df['date'] <= end_date]

        return df

    def load_ohlcv(self, code: str,
                   start_date: str = None,
                   end_date: str = None) -> Optional[pd.DataFrame]:
        """
        OHLCV만 추출 (기존 TradingEngine 호환)

        Returns:
            DataFrame [timestamp, open, high, low, close, volume]
        """
        df = self.load(code, start_date, end_date)
        if df is None:
            return None

        result = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        result = result.rename(columns={'date': 'timestamp'})
        return result

    def load_with_indicators(self, code: str,
                             start_date: str = None,
                             end_date: str = None) -> Optional[pd.DataFrame]:
        """
        전체 컬럼 (지표 포함) 로드 - 지표 재계산 불필요

        CSV에 이미 계산된 지표:
          MA5/20/60/120, RSI, MACD, 볼린저, ATR, 스토캐스틱, OBV,
          ADX, 외국인/기관 순매매, EMA, TRIX
        """
        return self.load(code, start_date, end_date)

    def to_5min_simulation(self, daily_df: pd.DataFrame,
                           bars_per_day: int = 78) -> pd.DataFrame:
        """
        일봉 데이터를 5분봉으로 시뮬레이션 변환 (백테스트용)

        각 일봉을 장중 가격 움직임으로 분해:
        - open에서 시작 → 장중 랜덤워크 → close로 수렴
        - high/low는 장중 최고/최저로 유지
        """
        import numpy as np

        frames = []
        for _, row in daily_df.iterrows():
            date = row['date'] if 'date' in row.index else row.get('timestamp', row.name)
            date = pd.to_datetime(date)

            o, h, l, c, v = (float(row['open']), float(row['high']),
                             float(row['low']), float(row['close']),
                             float(row['volume']))

            # 5분봉 타임스탬프 생성
            times = pd.date_range(
                f"{date.date()} 09:00", periods=bars_per_day, freq="5min"
            )

            # 가격 경로 생성 (open → close)
            n = len(times)
            prices = np.linspace(o, c, n)

            # 노이즈 추가 (high/low 범위 내)
            range_size = h - l
            if range_size > 0:
                noise = np.random.normal(0, range_size * 0.1, n)
                prices = np.clip(prices + noise, l, h)
                prices[0] = o
                prices[-1] = c

            # 각 5분봉의 OHLCV 생성
            candles = []
            vol_per_bar = max(1, int(v / n))

            for i in range(n):
                bar_noise = abs(np.random.normal(0, range_size * 0.02))
                bar_o = prices[i]
                bar_c = prices[min(i + 1, n - 1)] if i < n - 1 else c
                bar_h = max(bar_o, bar_c) + bar_noise
                bar_l = min(bar_o, bar_c) - bar_noise
                bar_h = min(bar_h, h)
                bar_l = max(bar_l, l)
                bar_v = int(np.random.exponential(vol_per_bar) + 100)

                candles.append({
                    'timestamp': times[i],
                    'open': round(bar_o),
                    'high': round(bar_h),
                    'low': round(bar_l),
                    'close': round(bar_c),
                    'volume': bar_v,
                })

            frames.append(pd.DataFrame(candles))

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def search_by_name(self, keyword: str) -> List[Tuple[str, str]]:
        """종목명으로 검색"""
        self._build_file_map()
        results = []
        for code in self._file_map:
            name = self.get_code_name(code)
            if keyword.lower() in name.lower():
                results.append((code, name))
        return sorted(results, key=lambda x: x[1])

    def get_top_volume(self, date: str = None, n: int = 20) -> List[Tuple[str, str, float]]:
        """거래량 상위 종목 추출"""
        self._build_file_map()
        volumes = []
        for code in self._file_map:
            df = self.load(code)
            if df is None or df.empty:
                continue
            if date:
                target_date = pd.to_datetime(date)
                row = df[df['date'] == target_date]
                if row.empty:
                    continue
                vol = float(row['volume'].iloc[0])
            else:
                vol = float(df['volume'].iloc[-1])
            name = self.get_code_name(code)
            volumes.append((code, name, vol))

        volumes.sort(key=lambda x: x[2], reverse=True)
        return volumes[:n]
