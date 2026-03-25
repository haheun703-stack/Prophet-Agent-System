# DATA_COLLECTION_OPTIMIZE.md
# 데이터 수집 파이프라인 최적화 체크리스트
#
# 사용법: Claude Code 세션 시 이 파일 읽고 미완료 항목부터 진행.
#
# 현재: 1,567종목 수집 = 220분 (3시간 40분)
# 목표: 84분 이하 (62% 절감)
# 핵심: F3(크래시방지) → P2(수급절반) → P4(Parquet병렬) → C1/C2(경로수정)

---

## DC-01: KIS 세션 방어 (F3 — CRITICAL, 크래시 방지)
상태: [x]
파일: flow_collector.py (또는 KIS 세션 생성하는 파일)
문제: _get_kis_session()에 try/except 없음
     mojito 초기화 실패 시 전체 collect_all이 크래시
     → 수급 수집 전체 0건 처리
수정:
  ```python
  def _get_kis_session(self):
      """KIS API 세션 생성 — 실패 시 None 반환, 크래시 방지"""
      try:
          session = ... # 기존 mojito 초기화 로직
          return session
      except Exception as e:
          logger.error(f"[KIS] 세션 생성 실패: {e}")
          # 1회 재시도 (토큰 만료일 수 있음)
          try:
              time.sleep(2)
              session = ... # 동일 로직 재시도
              logger.info("[KIS] 세션 재생성 성공")
              return session
          except Exception as e2:
              logger.critical(f"[KIS] 세션 재생성도 실패: {e2}")
              return None
  ```
  
  세션 사용하는 모든 곳에 None 체크 추가:
  ```python
  session = self._get_kis_session()
  if session is None:
      logger.error("[FLOW] KIS 세션 없음 — 수급 수집 스킵")
      return  # 크래시 대신 스킵
  ```
테스트: mojito import 경로를 일시적으로 잘못 설정 → 크래시 안 하고 스킵하는지
효과: 세션 실패해도 봇이 죽지 않음

---

## DC-02: 수급 수집 통합 루프 (P2 — 111분→55분)
상태: [x]
파일: flow_collector.py
문제: investor_flow(1,567종목) 후 foreign_exh(1,567종목) 순차 실행
     = 3,134 API 호출 × 2.1초 = 111분
수정:
  2개 API를 1개 루프에서 인터리빙:

  ```python
  def collect_flow_combined(self, codes: list):
      """
      투자자 수급 + 외인 소진율을 한 루프에서 수집
      
      Before: 
        for code in codes: fetch_investor(code); sleep(0.12)
        for code in codes: fetch_foreign(code); sleep(0.12)
        → 2 × 1567 × (2.1 + 0.12) = 6,683초
      
      After:
        for code in codes:
          fetch_investor(code); sleep(0.06)
          fetch_foreign(code); sleep(0.06)
        → 1567 × (2.1 + 0.06 + 2.1 + 0.06) = ~6,700초?
        
      아니, 이렇게 하면 절감 안 됨. 핵심은 다른 접근:
      """
      # 방법 A: asyncio로 2개 API 동시 호출
      # KIS API는 tr_id가 다르면 동시 호출 가능
      # investor = tr_id FHKST01010900
      # foreign  = tr_id FHKST01010100
      
      import asyncio
      import aiohttp
      
      async def fetch_pair(session, code):
          """한 종목의 수급 + 소진율을 동시에 가져오기"""
          investor_task = self._async_fetch_investor(session, code)
          foreign_task = self._async_fetch_foreign(session, code)
          results = await asyncio.gather(investor_task, foreign_task, 
                                          return_exceptions=True)
          return code, results
      
      # 동시성 제한: 5개씩 (KIS rate limit 초당 20건 고려)
      semaphore = asyncio.Semaphore(5)
      
      async def limited_fetch(session, code):
          async with semaphore:
              result = await fetch_pair(session, code)
              await asyncio.sleep(0.05)  # 안전 마진
              return result
      
      # 실행
      async with aiohttp.ClientSession() as session:
          tasks = [limited_fetch(session, code) for code in codes]
          results = await asyncio.gather(*tasks)
      
      # 예상: 1567종목 × (2.1초 / 5동시) = ~660초 (11분!)
      # 111분 → 11분 = 90% 절감
  
  # 방법 B: 동기식이지만 2세션 활용 (asyncio 못 쓸 경우)
  # KIS 세션 2개 생성, ThreadPoolExecutor(2)로 병렬
  
  from concurrent.futures import ThreadPoolExecutor
  
  def collect_flow_threaded(self, codes):
      session1 = self._get_kis_session()  # investor용
      session2 = self._get_kis_session()  # foreign용
      
      def fetch_investor_batch(codes_chunk):
          for code in codes_chunk:
              try:
                  self._fetch_investor(session1, code)
                  time.sleep(0.12)
              except: pass
      
      def fetch_foreign_batch(codes_chunk):
          for code in codes_chunk:
              try:
                  self._fetch_foreign(session2, code)
                  time.sleep(0.12)
              except: pass
      
      with ThreadPoolExecutor(max_workers=2) as executor:
          f1 = executor.submit(fetch_investor_batch, codes)
          f2 = executor.submit(fetch_foreign_batch, codes)
          f1.result()
          f2.result()
      
      # 예상: 55분 (절반) — 2개가 동시에 돌아가므로
  
  # 권장: 방법 B가 안전. asyncio는 KIS SDK 호환성 확인 필요.
  # 방법 B만으로도 111분→55분 달성.
  ```

  주의사항:
  - KIS API 동시 세션 2개 허용 여부 확인
  - 같은 앱키로 2세션 안 되면 → 방법 B 대신 인터리빙 방식
  - rate limit 초당 20건 기준, 2스레드 × 8건/초 = 16건/초 → 안전
테스트: 수급 수집 총 시간 측정 (110분 → 55분 이하)
효과: 수급 수집 50% 절감

---

## DC-03: Parquet 빌드 병렬화 (P4 — 98분→25분)
상태: [x]
파일: extend_parquet_all() 있는 파일 (data 처리 모듈)
문제: 1,567종목 CSV→Parquet 변환이 단일 스레드 순차
     CPU 바운드 작업이라 ProcessPoolExecutor로 병렬화 가능
수정:
  ```python
  from concurrent.futures import ProcessPoolExecutor
  import multiprocessing
  
  def extend_parquet_all_parallel(self, codes: list, n_workers: int = 4):
      """
      Parquet 빌드 병렬화
      
      Before: 1567종목 순차 = 98분
      After: 4 프로세스 병렬 = ~25분
      """
      # 종목 리스트를 n_workers 청크로 분할
      chunk_size = len(codes) // n_workers + 1
      chunks = [codes[i:i+chunk_size] for i in range(0, len(codes), chunk_size)]
      
      with ProcessPoolExecutor(max_workers=n_workers) as executor:
          futures = [executor.submit(self._build_parquet_chunk, chunk) 
                     for chunk in chunks]
          
          results = []
          for future in futures:
              try:
                  result = future.result(timeout=1800)  # 30분 타임아웃
                  results.append(result)
              except Exception as e:
                  logger.error(f"[PARQUET] 청크 빌드 실패: {e}")
      
      # 결과 합산
      total_success = sum(r['success'] for r in results)
      total_fail = sum(r['fail'] for r in results)
      logger.info(f"[PARQUET] 완료: {total_success}성공, {total_fail}실패")
  
  def _build_parquet_chunk(self, codes_chunk: list) -> dict:
      """개별 청크 처리 (별도 프로세스에서 실행)"""
      success, fail = 0, 0
      for code in codes_chunk:
          try:
              self._extend_single_parquet(code)
              success += 1
          except Exception as e:
              logger.warning(f"[PARQUET] {code} 실패: {e}")
              fail += 1
      return {'success': success, 'fail': fail}
  ```

  n_workers 결정:
  - VPS CPU 코어 수 확인 (보통 2~4)
  - 코어 수 = n_workers (CPU 바운드이므로)
  - VPS가 2코어면 n_workers=2 → 98분→49분
  - VPS가 4코어면 n_workers=4 → 98분→25분
테스트: Parquet 빌드 총 시간 측정 (98분 → 25~49분)
효과: Parquet 빌드 60~75% 절감

---

## DC-04: VPS 공매도 CSV 배포 (C1)
상태: [x]
파일: VPS 서버 + short_analyzer 관련
문제: VPS에 공매도 CSV 0개 → 공매도 스코어링 작동 불가
수정:
  선택 1: 로컬→VPS rsync/scp로 공매도 CSV 배포
  선택 2: VPS에서 pykrx로 직접 수집하도록 스케줄 추가
  선택 3: 공매도 데이터 없으면 graceful skip (점수 0으로 처리)
  
  권장: 선택 3을 즉시 적용 (방어) + 선택 2를 이후 추가
  
  ```python
  # short_analyzer.py
  def analyze_short_interest(self, stock_code):
      try:
          data = self._load_short_data(stock_code)
          if data is None or len(data) == 0:
              return {'signal': 'NO_DATA', 'score_adjustment': 0}
          # ... 기존 분석 로직
      except Exception:
          return {'signal': 'NO_DATA', 'score_adjustment': 0}
  ```
테스트: VPS에서 공매도 분석 시 크래시 안 하는지
효과: 공매도 데이터 없어도 봇이 정상 작동

---

## DC-05: 뉴스 경로 수정 (C2)
상태: [x]
파일: data_verifier.py + news_ai_scanner.py
문제: NEWS_DIR 경로가 data_verifier.py와 실제 저장 경로 불일치
수정:
  1. news_ai_scanner.py에서 실제 뉴스 저장 경로 확인
  2. data_verifier.py의 NEWS_DIR 경로를 실제 경로로 수정
  3. 경로 불일치 시 os.path.exists 체크 로직 확인
테스트: data_verifier에서 news_sentiment 검증 PASS
효과: 데이터 검증 정확도 향상

---

## DC-06: 일봉 수집 병렬화 (P3 — 6분→1.5분)
상태: [x]
파일: 일봉 수집 함수 (pykrx 사용)
문제: pykrx 일봉 수집 순차 실행 = 6분
수정:
  ```python
  from concurrent.futures import ThreadPoolExecutor
  
  def collect_daily_pykrx_parallel(self, codes, n_workers=4):
      """
      pykrx 일봉 수집 병렬화 (IO 바운드 → ThreadPool)
      """
      def fetch_single(code):
          try:
              df = stock.get_market_ohlcv_by_date(start, end, code)
              self._save_daily_csv(code, df)
              return True
          except Exception as e:
              logger.warning(f"[DAILY] {code} 실패: {e}")
              return False
      
      with ThreadPoolExecutor(max_workers=n_workers) as executor:
          results = list(executor.map(fetch_single, codes))
      
      success = sum(results)
      logger.info(f"[DAILY] 완료: {success}/{len(codes)}")
  ```

  주의: pykrx가 thread-safe인지 확인 필요
  - pykrx 내부에서 requests 사용 → session 공유 문제 가능
  - 안전하게: 각 스레드에서 별도 import + 호출
테스트: 일봉 수집 시간 6분 → 1.5분
효과: 일봉 수집 75% 절감

---

## DC-07: Step1+Step2 동시 실행 (P1)
상태: [x]
파일: collect_all() 또는 메인 수집 함수
문제: Step1(일봉 pykrx)과 Step2(수급 KIS)가 순차 실행
     서로 다른 API/소스 → 동시 실행 가능
수정:
  ```python
  from concurrent.futures import ThreadPoolExecutor
  
  def collect_all_optimized(self):
      """Step1과 Step2를 동시에 실행"""
      with ThreadPoolExecutor(max_workers=2) as executor:
          f1 = executor.submit(self.collect_daily_pykrx_parallel, codes)
          f2 = executor.submit(self.collect_flow_combined, codes)
          
          f1.result()  # Step1 완료 대기
          f2.result()  # Step2 완료 대기
      
      # Step3: 국적별 (15초, 병렬화 불필요)
      self.collect_nationality()
      
      # Step4: Parquet (병렬)
      self.extend_parquet_all_parallel(codes)
      
      # Step5: 동기화
      self.sync_stock_data_daily()
  ```
테스트: 전체 수집 시간 측정 (220분 → 84분 이하)
효과: Step1이 Step2와 겹쳐서 -6분 추가 절감

---

## DC-08: 수집 시간 로깅 + 모니터링
상태: [x]
파일: collect_all() + bot_logger.py
문제: 수집 시간을 정확히 모르면 개선 효과 측정 불가
수정:
  각 단계별 시작/종료 시간 로깅:
  
  ```python
  import time
  
  def collect_all_optimized(self):
      timings = {}
      
      t0 = time.time()
      # Step 1+2 (동시)
      ...
      timings['step1_2'] = time.time() - t0
      
      t1 = time.time()
      # Step 3
      ...
      timings['step3'] = time.time() - t1
      
      t2 = time.time()
      # Step 4
      ...
      timings['step4'] = time.time() - t2
      
      t3 = time.time()
      # Step 5
      ...
      timings['step5'] = time.time() - t3
      
      timings['total'] = time.time() - t0
      
      # 로그 기록
      log_event('COLLECT', f"수집 완료: {timings['total']:.0f}초", timings)
      
      # 마감 리포트에 수집 시간 추가
      # "⏱ 수집: 84분 (수급55+Parquet25+기타4)"
  ```
  
  마감 리포트에 1줄 추가:
  ```
  ⏱ 수집: 84분 (Step1+2: 55분, Parquet: 25분, 기타: 4분)
  ```
테스트: 수집 완료 후 로그에 단계별 시간 기록
효과: 매일 수집 성능 모니터링 가능

---

# 최적화 예상 결과
#
# 단계        현재       최적화 후    절감
# Step 1      6분        1.5분       -4.5분 (DC-06)
# Step 2      111분      55분        -56분 (DC-02)
# Step 1+2    -          겹침        -1.5분 (DC-07)  
# Step 3      0.3분      0.3분       0
# Step 4      98분       25분        -73분 (DC-03)
# Step 5      4분        2분         -2분
# 합계        220분      ~82분       -138분 (63% 절감)
#
# 16:00 시작 → 17:22 완료 → 16:25 데이터검증에는 못 맞추지만
# 17:30 저녁분석에는 맞음
#
# 향후 유니버스 티어링(~100종목 매일 풀수집)하면 → 10분 이하 가능

---

# 완료 기록
# 2026-03-25 DC-01~DC-08 전체 완료
# 수정 파일:
#   - flow_collector.py: DC-01(세션방어) + DC-02(수급 병렬)
#   - extend_parquet_data.py: DC-01(세션방어) + DC-03(Parquet 병렬 + KIS스킵)
#   - short_analyzer.py: DC-04(만료 체크 14일)
#   - data_verifier.py: DC-04(SHORT SKIP) + DC-05(뉴스경로+옵션검증)
#   - universe_builder.py: DC-06(일봉 ThreadPool 4 workers)
#   - collect_all.py: DC-07(Step1+2 동시) + DC-08(단계별 타이밍)
#   - telegram_bot.py: DC-02(수급병렬) + DC-03(Parquet KIS스킵) + DC-07(동시실행) + DC-08(시간로깅)
#
# 핵심 발견: Parquet 빌드 98분의 원인은 KIS API 재호출이었음
#   flow_collector가 이미 수집한 데이터를 Parquet 빌더가 다시 호출
#   skip_kis_fill=True로 KIS 재호출 제거 → 98분 → ~5분 예상
