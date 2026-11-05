"""M6: build the daily HTML report from the gold layer.

Everything is collected through Spark (so it scales), then rendered as one
self-contained HTML file. Plotly.js comes from a CDN, which keeps the Spark
image dependency-free - no pip install, no kaleido.

Output: /data/reports/daily_<YYYYMMDD>.html
"""
import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD = "/data/lake/gold"
OPS = "/data/ops"
OUT_DIR = "/data/reports"


def read_history(path: str, limit: int = 12) -> list:
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return rows[-limit:]


HTML_TMPL = """<!DOCTYPE html>
<html lang="zh-Hant"><head>
<meta charset="utf-8"><title>晶圓廠日報 __DATE__</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  :root { --bg:#0d1117; --panel:#161b22; --line:#30363d; --txt:#e6edf3; --dim:#8b949e; --good:#3fb950; --warn:#d29922; --bad:#f85149; }
  body { margin:0; background:var(--bg); color:var(--txt); font-family:"Segoe UI","Microsoft JhengHei",sans-serif; }
  header { padding:24px 32px 8px; }
  h1 { margin:0; font-size:22px; font-weight:600; }
  .sub { color:var(--dim); font-size:13px; margin-top:6px; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px; padding:20px 32px; }
  .card { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:16px 18px; }
  .card .k { color:var(--dim); font-size:12px; letter-spacing:.04em; }
  .card .v { font-size:26px; font-weight:600; margin-top:6px; }
  .card .u { color:var(--dim); font-size:13px; margin-left:4px; }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; padding:0 32px 32px; }
  .plot { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:8px; min-height:420px; }
  footer { color:var(--dim); font-size:12px; padding:0 32px 28px; }
  .ok { color:var(--good); } .warn { color:var(--warn); } .bad { color:var(--bad); }
  .plot-head { display:flex; align-items:center; justify-content:space-between;
               gap:12px; padding:10px 12px 0; flex-wrap:wrap; }
  .plot-head .t { font-size:13px; font-weight:600; }
  .legend { display:flex; align-items:center; gap:14px; font-size:12px; color:var(--dim); }
  .legend span { display:flex; align-items:center; gap:6px; }
  .legend i { width:10px; height:10px; border-radius:2px; display:inline-block; }
  #p4 { height:372px; }
  @media (max-width:900px) { .grid { grid-template-columns:1fr; } }
</style></head><body>
<header>
  <h1>晶圓廠營運日報</h1>
  <div class="sub">資料日期 __DATE__　·　產生時間 __GENERATED__　·　管線狀態
    <span class="__STATUS_CLASS__">__STATUS__</span></div>
</header>

<section class="cards">
__CARDS__
</section>

<div class="grid">
  <div class="plot" id="p1"></div>
  <div class="plot" id="p2"></div>
  <div class="plot" id="p3"></div>
  <div class="plot" style="padding:0">
    <div class="plot-head">
      <div class="t">最近執行的步驟耗時（秒）</div>
      <div class="legend">
        <span><i style="background:#3fb950"></i>SUCCESS 成功</span>
        <span><i style="background:#d29922"></i>FAILED_DQ 資料品質未通過</span>
        <span><i style="background:#f85149"></i>FAILED 程式錯誤</span>
      </div>
    </div>
    <div id="p4"></div>
  </div>
</div>
<section style="padding:0 32px 24px">
  <details style="background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 16px">
    <summary style="cursor:pointer;color:var(--dim)">原始序列（用來確認資料真的在頁面裡）</summary>
    <pre id="raw" style="color:var(--txt);font-size:12px;white-space:pre-wrap"></pre>
  </details>
</section>
<footer>資料來源：bronze/silver/gold 三層管線（Spark）· 指標定義見 docs/METRICS.md · 維運規則見 docs/OPERATIONS.md</footer>
<script>
const D = __PAYLOAD__;

// A fresh layout for every chart. Plotly resolves axes in place, so reusing one
// shared axis object leaks settings between charts (a category y-axis from the
// horizontal bar chart turned the next two charts into category axes too).
function L(over) {
  const base = {
    paper_bgcolor: "#161b22", plot_bgcolor: "#161b22",
    font: { color: "#e6edf3", size: 12 },
    margin: { l: 62, r: 20, t: 44, b: 60 },
    xaxis: { gridcolor: "#30363d", zerolinecolor: "#30363d", automargin: true },
    yaxis: { gridcolor: "#30363d", zerolinecolor: "#30363d", automargin: true }
  };
  const merged = Object.assign(base, over || {});
  return merged;
}
const CFG = { displayModeBar: false, responsive: true };

function draw(id, traces, over) {
  const el = document.getElementById(id);
  try {
    Plotly.newPlot(el, traces, L(over), CFG);
  } catch (e) {
    el.innerHTML = '<div style="padding:40px;color:#f85149">' + id + " 繪圖失敗：" + e + "</div>";
  }
}

draw("p1", [{
  type: "bar",
  x: D.hist.map(r => r.bucket),
  y: D.hist.map(r => r.count),
  marker: { color: "#58a6ff" },
  name: "晶圓數"
}], { title: "良率分佈（每 5% 一組）",
      xaxis: { title: "良率下限 (%)", gridcolor: "#30363d", automargin: true },
      yaxis: { title: "晶圓數", gridcolor: "#30363d", automargin: true } });

draw("p2", [{
  type: "bar", orientation: "h",
  x: D.topStill.map(r => r.still_in_process),
  y: D.topStill.map(r => r.lot_id),
  marker: { color: "#d29922" },
  name: "未出站"
}], { title: "未出站事件最多的批次",
      margin: { l: 110, r: 24, t: 44, b: 60 },
      xaxis: { title: "未出站事件筆數", gridcolor: "#30363d", automargin: true },
      yaxis: { gridcolor: "#30363d", automargin: true } });

draw("p3", [{
  type: "scatter", mode: "lines+markers",
  x: D.daily.map(r => r.day),
  y: D.daily.map(r => r.events),
  line: { color: "#3fb950", width: 2 },
  marker: { size: 4 },
  name: "每日事件數"
}], { title: "每日事件數",
      xaxis: { type: "category", nticks: 14, tickangle: -45, gridcolor: "#30363d", automargin: true },
      yaxis: { title: "事件筆數", gridcolor: "#30363d", automargin: true } });

// Numeric x positions: the step names repeat across runs, and a category axis
// would merge those repeats into one slot (bars stacked on top of each other).
const runsX = D.runs.map((r, i) => i);
const runShapes = [];
const runNotes = [];
for (let i = 1; i < D.runs.length; i++) {
  if (D.runs[i].run_id !== D.runs[i - 1].run_id) {
    runShapes.push({ type: "line", xref: "x", yref: "paper",
                     x0: i - 0.5, x1: i - 0.5, y0: 0, y1: 1,
                     line: { color: "#8b949e", width: 1, dash: "dot" } });
  }
}
let gs = 0;
for (let i = 1; i <= D.runs.length; i++) {
  if (i === D.runs.length || D.runs[i].run_id !== D.runs[gs].run_id) {
    runNotes.push({ xref: "x", yref: "paper", x: (gs + i - 1) / 2, y: 1.04,
                    text: String(D.runs[gs].run_id || "").replace("T", " ").slice(0, 15),
                    showarrow: false, font: { size: 10, color: "#8b949e" } });
    gs = i;
  }
}
draw("p4", [{
  type: "bar",
  x: runsX,
  y: D.runs.map(r => r.duration_sec),
  marker: { color: D.runs.map(r => r.status === "SUCCESS" ? "#3fb950" : (r.status === "FAILED_DQ" ? "#d29922" : "#f85149")) },
  name: "耗時"
}], { margin: { l: 62, r: 20, t: 34, b: 70 },
      shapes: runShapes,
      annotations: runNotes,
      xaxis: { type: "linear", tickmode: "array", tickvals: runsX,
               ticktext: D.runs.map(r => r.step), tickangle: -40,
               range: [-0.6, D.runs.length - 0.4],
               gridcolor: "#30363d", automargin: true },
      yaxis: { title: "秒", gridcolor: "#30363d", automargin: true } });

document.getElementById("raw").textContent = JSON.stringify({
  lengths: { hist: D.hist.length, topStill: D.topStill.length, daily: D.daily.length, runs: D.runs.length },
  daily: D.daily.slice(0, 5).concat(D.daily.slice(-2)),
  runs: D.runs.slice(0, 5).concat(D.runs.slice(-2))
}, null, 2);
</script></body></html>
"""


def _cards(kpi: dict) -> str:
    items = [
        ("製程事件", f"{kpi['events']:,}", ""),
        ("晶圓片數", f"{kpi['wafers']:,}", ""),
        ("平均良率", f"{kpi['yield_mean']}", "%"),
        ("低於 90% 的晶圓", f"{kpi['low_yield']:,}", ""),
        ("仍在機台上", f"{kpi['still']:,}", "筆"),
    ]
    out = []
    for label, value, unit in items:
        u = f'<span class="u">{unit}</span>' if unit else ""
        out.append(f'  <div class="card"><div class="k">{label}</div>'
                   f'<div class="v">{value}{u}</div></div>')
    return "\n".join(out)


def render(kpi: dict, hist: list, top_still: list, daily: list, runs: list) -> str:
    payload = json.dumps(
        {"hist": hist, "topStill": top_still, "daily": daily, "runs": runs},
        ensure_ascii=False,
    )
    html = HTML_TMPL
    for token, value in (
        ("__PAYLOAD__", payload),
        ("__CARDS__", _cards(kpi)),
        ("__DATE__", str(kpi["date"])),
        ("__GENERATED__", kpi["generated"]),
        ("__STATUS__", str(kpi["status"])),
        ("__STATUS_CLASS__", kpi["status_class"]),
    ):
        html = html.replace(token, value)
    return html


def main() -> None:
    spark = (
        SparkSession.builder.appName("50_build_report")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    os.makedirs(OUT_DIR, exist_ok=True)

    y = spark.read.parquet(f"{GOLD}/wafer_yield")
    s = spark.read.parquet(f"{GOLD}/lot_event_summary")
    t = spark.read.parquet(f"{GOLD}/tool_load_hourly")

    hist = [
        {"bucket": int(r["bucket"]), "count": int(r["count"])}
        for r in y.withColumn("bucket", (F.floor(F.col("yield_pct") / 5) * 5).cast("int"))
        .groupBy("bucket").count().orderBy("bucket").collect()
    ]
    top_still = [
        {"lot_id": r["lot_id"], "still_in_process": int(r["still_in_process"])}
        for r in s.orderBy(F.desc("still_in_process")).limit(10).collect()
    ]
    daily = [
        {"day": r["day"], "events": int(r["events"])}
        for r in t.withColumn("day", F.date_format("hour_ts", "yyyy-MM-dd"))
        .groupBy("day").agg(F.sum("event_cnt").alias("events")).orderBy("day").collect()
    ]
    hist_path = f"{OPS}/run_history.jsonl"
    print(f"[report] history path={hist_path} exists={os.path.exists(hist_path)}")
    runs = read_history(hist_path, limit=14)

    kpi_row = y.agg(
        F.count("*").alias("wafers"),
        F.round(F.avg("yield_pct"), 2).alias("yield_mean"),
        F.sum(F.when(F.col("yield_pct") < 90.0, 1).otherwise(0)).alias("low_yield"),
    ).first()
    events = t.select(F.sum("event_cnt")).first()[0]
    still = s.select(F.sum("still_in_process")).first()[0]

    last_run = runs[-1] if runs else {"status": "UNKNOWN", "run_id": "-"}
    status = last_run.get("status", "UNKNOWN")
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    kpi = {
        "date": daily[-1]["day"] if daily else date_str,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "status": status,
        "status_class": "ok" if status == "SUCCESS" else ("warn" if status == "FAILED_DQ" else "bad"),
        "events": int(events or 0),
        "wafers": int(kpi_row["wafers"]),
        "yield_mean": kpi_row["yield_mean"],
        "low_yield": int(kpi_row["low_yield"] or 0),
        "still": int(still or 0),
    }

    print(f"[report] series lengths: hist={len(hist)} top_still={len(top_still)} "
          f"daily={len(daily)} runs={len(runs)}")
    if hist:
        print(f"[report]   hist[0]={hist[0]} hist[-1]={hist[-1]}")
    if daily:
        print(f"[report]   daily[0]={daily[0]} daily[-1]={daily[-1]}")
    if runs:
        print(f"[report]   runs[-1]={runs[-1]}")
    with open(f"{OUT_DIR}/daily_{datetime.now(timezone.utc).strftime('%Y%m%d')}.json",
              "w", encoding="utf-8") as fh:
        json.dump({"hist": hist, "topStill": top_still, "daily": daily, "runs": runs},
                  fh, ensure_ascii=False, indent=1)

    html = render(kpi, hist, top_still, daily, runs)
    path = f"{OUT_DIR}/daily_{date_str}.html"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"[report] wrote {path} ({len(html):,} bytes)")
    print(f"[report] wafers={kpi['wafers']:,} events={kpi['events']:,} "
          f"yield_mean={kpi['yield_mean']} still={kpi['still']:,} status={kpi['status']}")
    spark.stop()


if __name__ == "__main__":
    main()
