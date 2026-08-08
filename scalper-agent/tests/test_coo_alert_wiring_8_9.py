# -*- coding: utf-8 -*-
"""[F-127] TradingCOO 알림 배선 검증 (8/9).

배경: `TradingCOO.__init__`은 `bot`/`auto_trader`/`cfo`/`cto`만 설정하는데,
알림 job 4곳이 **정의되지 않은** `self._send_alert`·`self.trader`를 썼다.
전부 `if not X: return`(보고할 게 없으면 조기 반환) **뒤**에 있어서
**할 말이 있을 때만** AttributeError로 죽었다 — 실패 양상이 뒤집혀 있다.

VPS 실측(14일): 봇시야 140건 · 상한가연속 52건 · 섹터동조 26건.
같은 파일이 이미 21곳에서 `getattr(self.auto_trader, "_send_alert", None)`을 쓴다.

이 테스트가 지키는 것: **알림 job이 COO에 없는 속성을 만지지 않는다.**
AST로 판정하므로 새 job이 같은 실수를 해도 잡힌다.

실행:
    python -X utf8 scalper-agent/tests/test_coo_alert_wiring_8_9.py
"""

import ast
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent          # scalper-agent/
ROOT = BASE_DIR.parent
COO = BASE_DIR / "bot" / "trading_coo.py"

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_fails: list = []
_total = 0

# COO 인스턴스가 실제로 갖는 속성 (=__init__ 에서 설정하는 것)
KNOWN = {"bot", "auto_trader", "cfo", "cto", "name", "success", "elapsed",
         "error", "timestamp"}
# 알림 job 이 잘못 만졌던 것들
FORBIDDEN = {"_send_alert", "trader"}


def check(name, got, want):
    global _total
    _total += 1
    ok = got == want
    print(f"  {'✅' if ok else '🚨'} {name}" + ("" if ok else f" — 실측 {got!r} / 기대 {want!r}"))
    if not ok:
        _fails.append(name)


def _self_attrs(src: str, cls_name: str = "TradingCOO"):
    """클래스 메서드 본문에서 읽는 `self.<attr>` 이름 → {attr: [line, ...]}"""
    tree = ast.parse(src)
    out: dict = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name == cls_name):
            continue
        for sub in ast.walk(node):
            if (isinstance(sub, ast.Attribute) and isinstance(sub.value, ast.Name)
                    and sub.value.id == "self" and isinstance(sub.ctx, ast.Load)):
                out.setdefault(sub.attr, []).append(sub.lineno)
    return out


def _assigned_attrs(src: str, cls_name: str = "TradingCOO") -> set:
    """`self.X = ...` **와 클래스 본문 상수** 둘 다 수집.

    ★8/9 테스트 자체 정정 — 처음엔 `self.X =` 만 봐서 `G4_JOBS`·`_RECOVERY_TIMEOUT`·
    `_RECOVERY_TOTAL_TIMEOUT`(전부 클래스 속성)을 '미정의'로 오탐했다. 코드가 옳고
    내 수집 범위가 좁았다."""
    tree = ast.parse(src)
    out = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name == cls_name):
            continue
        # 클래스 본문 직속 대입 (G4_JOBS = {...} 같은 상수)
        for stmt in node.body:
            if isinstance(stmt, ast.Assign):
                for t in stmt.targets:
                    if isinstance(t, ast.Name):
                        out.add(t.id)
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                out.add(stmt.target.id)
        for sub in ast.walk(node):
            if (isinstance(sub, ast.Attribute) and isinstance(sub.value, ast.Name)
                    and sub.value.id == "self" and isinstance(sub.ctx, ast.Store)):
                out.add(sub.attr)
    return out


# ══════════════════════════════════════════════════════════════════
def test_01_금지속성_참조_0건():
    print("\n[1] 현재 코드 — COO 에 없는 속성 참조 0건")
    src = COO.read_text(encoding="utf-8")
    reads = _self_attrs(src)
    for attr in sorted(FORBIDDEN):
        hits = reads.get(attr, [])
        check(f"self.{attr} 참조", hits, [])


def test_02_읽는_속성은_전부_대입됨():
    print("\n[2] 읽는 self.<attr> 는 전부 어딘가에서 대입된다 (미정의 사냥)")
    src = COO.read_text(encoding="utf-8")
    reads = _self_attrs(src)
    assigned = _assigned_attrs(src) | KNOWN
    # 메서드명은 제외 (self.foo() 호출)
    tree = ast.parse(src)
    methods = {n.name for n in ast.walk(tree)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    missing = {a: ls for a, ls in reads.items()
               if a not in assigned and a not in methods}
    check("미정의 속성", sorted(missing), [])


def test_03_정본_패턴_사용():
    print("\n[3] 알림은 getattr 정본 패턴으로만")
    src = COO.read_text(encoding="utf-8")
    check("getattr(auto_trader,_send_alert) 사용",
          src.count('getattr(self.auto_trader, "_send_alert", None)') >= 24, True)
    check("getattr(auto_trader,trader) 사용",
          'getattr(self.auto_trader, "trader", None)' in src, True)


def test_04_음성대조_구코드():
    print("\n[4] ★음성 대조 — fix 전 코드에서 실제로 잡히는가")
    rel = "scalper-agent/bot/trading_coo.py"
    try:
        old = subprocess.run(["git", "show", f"HEAD:{rel}"], cwd=ROOT,
                             capture_output=True, timeout=30).stdout.decode("utf-8")
    except Exception as e:  # noqa: BLE001
        print(f"  🚨 미검증 — git show 실패({e}). 음성 대조가 돌지 않았다(통과 아님).")
        _fails.append("음성대조 미실행")
        return
    if not old.strip():
        print("  🚨 미검증 — 구코드 판독 실패(통과 아님).")
        _fails.append("음성대조 미실행")
        return
    reads = _self_attrs(old)
    # ★테스트 자체 정정 — 처음엔 4(=사이트 수)를 기대했으나 각 사이트가
    #   `if self._send_alert:` + `await self._send_alert(...)` 로 **2회** 참조한다.
    #   4 사이트 × 2 = 8 이 맞다. 코드가 옳고 내 기대가 틀렸다.
    check("구코드 self._send_alert 참조 8(=4사이트×2)", len(reads.get("_send_alert", [])), 8)
    check("구코드 self.trader 참조 2", len(reads.get("trader", [])), 2)


def main() -> int:
    import inspect
    tests = [fn for name, fn in sorted(globals().items())
             if name.startswith("test_") and inspect.isfunction(fn)]
    for t in tests:
        t()
    print("\n" + "=" * 58)
    if _fails:
        print(f"🚨 {_total}건 중 실패 {len(_fails)}건: {', '.join(_fails)}")
        return 1
    print(f"✅ 전건 PASS ({_total}/{_total} · 발견 {len(tests)}함수)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
