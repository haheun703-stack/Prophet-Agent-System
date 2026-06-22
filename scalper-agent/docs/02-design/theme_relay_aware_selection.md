# 테마 relay-aware 종목선정 설계 (6/22) — paper shadow only

> 단타봇 설계 스펙 → 사장님 승인 → 구현 → 검증. 이 문서는 스펙(코드 아님).
> ★ 1차 구현 = **paper shadow only / 기록 전용**. 실매수 로직·tier·SAJANG·order path 무접촉. 봇 OFF 유지. ★
> 결정: 사장님 + Codex 6/22 — 수동 theme registry + manual seed + marketcap fallback + paper shadow 시작.

## 0. 대전제 (불변)
- **목적**: "대장주랑 소부장을 같이 봐서, 강한 테마 안에서 그 시점 주도하는 쪽을 초입에 잡는다." (사장님 6/22)
- ★ **고정 룰 금지**: '소부장 먼저' 고정 X / '대장주 우선' 고정 X / 시총 단독 분류 X / 실매수 후보점수 즉시변경 X.
- ★ **관측 우선**: relay 방향은 박지 않고 매일 **관측**(relay_leader_group). 검증 없이 flip 금지(5/26·5/31 교훈).

## 1. 데이터 근거 (오늘 검증, tools/multi_theme_relay_6_22.py)
6/4~6/19 5개 테마 실측 — **relay는 보편 법칙 아님, 테마마다 주도구조 다름**:
| 테마 | 패턴 | 초입(6/4→6/12) / 후반(6/12→6/19) |
|---|---|---|
| 반도체 | ✔ relay (소부장→대장주) | 소부장 +9.4% → 대장주 +25.5% |
| 자동차 | 소부장 지속 우위 | 소부장 +2.0/+6.4 (대장주 음수) |
| 방산 | 대장주 지속 우위 | 대장주 +4.6/+1.7 (소부장 음수) |
| 로봇·ESS | 테마 약세 (회피) | 둘 다 −15~31% 손실 |

- ★ **role ≠ size**: 에코프로비엠 20조·포스코퓨처엠 22조는 소부장(2차전지 소재)인데 로봇 대장주 두산로보틱스(6.9조)보다 큼. **시총 분류는 오판** → manual seed 필수.
- ★ 6/12→6/19만 보면 소부장이 패자로 보이나, 실은 6/4 먼저 가서 끝물이었음(단타봇 6/12 꼭지 매수 −18% 오판의 정체).
- 한계(정직): 1구간·n 작음(로봇/방산 대장주 n2). "테마마다 다르다"는 견고하나, 어느 테마가 강한지는 시점마다 변함 → 관측으로.

## 2. 설계 — 4단 (전부 관측·기록만)
```
[강한 테마 식별] → [테마 내 group 분류] → [주도 group 관측] → [초입 후보] → [바통 감시]
 theme_strength      registry(seed+fallback)   relay_leader_group   early_candidates  rotation_watch
```
1. **theme_strength** — registry 각 테마 구성종목의 momentum/breadth 집계. 음수=약세→회피 기록. (sector_relay 분류 단일진실 재사용 가능.)
2. **intra_theme_group_strength** — 강한 테마 안에서 leaders group vs supply_chain group 상대강도(기간 수익률·당일 모멘텀) 산출.
3. **relay_leader_group** — 그 시점 주도 group(LEADERS / SUPPLY_CHAIN) 판정 + 직전 대비 역전 여부(바통). ★ registry에 방향 박지 않고 강도로 결정.
4. **early_candidates** — 주도 group에서 `early_variant_shadow`(pos20 바닥·거래량 초입) 통과 종목 = 초입 후보. 꼭지(6/12 한미반도체류) 회피.
5. **rotation_watch** — leader group 역전 시(반도체 6/12류) 로테이션 시그널 기록. (관측만, 실행 X.)

## 3. registry 스펙 (`data/theme_relay_registry.json`, 생성 완료)
- 테마별 `leaders`(주도 대표주) / `supply_chain`(공급망) **manual seed** + `marketcap_fallback_조`.
- group 분류: **seed 우선** → seed에 없는 테마 멤버만 fallback(시총 이상=leader 후보, 미만=supply). 시총 단독 금지.
- `principles`에 role_not_size / no_hardcoded_relay / theme_strength_gate 명문화.
- 유지보수: version/updated_at/source. 테마·종목 변하면 갱신(수동).

## 4. shadow 기록 필드 (`data/theme_relay_shadow.py`, 다음 구현)
테마·날짜별 1레코드(멱등): `theme`, `date`, `theme_strength`, `leaders_strength`, `supply_strength`,
`intra_theme_group_strength`(상대강도), `relay_leader_group`(+직전대비 역전flag), `early_candidates`(주도group 초입종목 list),
`rotation_watch`(역전 시그널), + forward(d1/d3/d5·mfe/mae, 주도group 평균). **picks·매수 무반영.**

## 5. 안전 불변식 (기존 shadow와 동일)
- read-only: daily csv + registry json + early_variant_shadow.json 만. 네트워크 0.
- record-only: shadow json만 기록. 매수/매도/picks/recommendation/SAJANG/order_intent **0 접촉**.
- 봇 OFF·실주문 0. ON/OFF 플래그 없음(생성기는 관측만).
- nightly 배선 시 is_trading_day 가드 + 비차단 try/except (기존 shadow 표준).

## 6. 1차 구현 범위 = paper shadow ONLY (Codex 6/22 스코프)
✅ 한다: sector_relay + early_variant_shadow 재사용 / theme_strength / intra_theme_group_strength /
  relay_leader_group / early_candidates / rotation_watch **기록만**.
❌ 절대 안 한다: 실매수 로직 변경 / tier 변경 / SAJANG·order path 접촉 / 실매수 후보점수 즉시변경 /
  소부장 우선 고정 / 대장주 우선 고정 / 시총 단독 group 분류.

## 7. 사장님 결정 (확정 — 6/22)
1. 테마 정의 = **수동 registry**(sector_relay 재사용 + 실전 테마 별도 박음). ✅
2. group 분류 = **manual seed + 시총 fallback**(시총 단독 금지). ✅
3. 첫 적용 = **무조건 paper shadow, 매수 flip 금지**. ✅

## 8. 산출물 / 순서
1. `docs/02-design/theme_relay_aware_selection.md` (이 문서) ✅
2. `data/theme_relay_registry.json` ✅
3. → 사장님 검토/승인 후 `data/theme_relay_shadow.py`(record-only) + 러너 + nightly 배선 + 셀프테스트 + 4-Tier.
4. → 2주+ 관측 → relay 패턴 일관성·early_candidates forward 검증 → 사장님 flip 결정(관측 없이 flip 금지).

## 9. 검증 계획 (구현 후)
- 셀프테스트: registry 로드·group 분류(seed 우선·fallback)·강도 산출·멱등·리터럴0.
- 회귀: 게이트 8/8 / 매도 무손상 / picks 불변 / SAJANG 무변경.
- forward: relay_leader_group이 실제 forward 우위 group과 맞는지(반도체 6/12 바통 재현).
- 정직: 절대수익 생존편향 — 상대비교만 신뢰. 약세 테마 회피가 1차 엣지.
