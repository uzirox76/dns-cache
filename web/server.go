package web

import (
	"context"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"time"

	"dns-cache/stats"
)

type Server struct {
	listen    string
	store     stats.Store
	startedAt time.Time
	srv       *http.Server
}

func New(listen string, store stats.Store) *Server {
	return &Server{
		listen:    listen,
		store:     store,
		startedAt: time.Now(),
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.dashboard)
	mux.HandleFunc("/api/stats", s.apiStats)

	s.srv = &http.Server{
		Addr:    s.listen,
		Handler: mux,
	}

	go func() {
		log.Printf("[web] listening on %s", s.listen)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[web] error: %v", err)
		}
	}()

	return nil
}

func (s *Server) Stop() {
	if s.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.srv.Shutdown(ctx)
	}
}

func (s *Server) apiStats(w http.ResponseWriter, r *http.Request) {
	snap := stats.BuildSnapshot(s.store, s.startedAt)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snap)
}

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	snap := stats.BuildSnapshot(s.store, s.startedAt)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl.Execute(w, snap)
}

var tmpl = template.Must(template.New("dashboard").Funcs(template.FuncMap{
	"percent": func(a, b int) float64 {
		if b == 0 {
			return 0
		}
		return float64(a) / float64(b) * 100
	},
	"colorForType": func(t string) string {
		switch t {
		case "A":
			return "#38bdf8"
		case "AAAA":
			return "#a78bfa"
		case "MX":
			return "#facc15"
		case "TXT":
			return "#22d3ee"
		case "CNAME":
			return "#4ade80"
		case "NS":
			return "#fb923c"
		case "SOA":
			return "#f87171"
		case "SRV":
			return "#e879f9"
		default:
			return "#94a3b8"
		}
	},
}).Parse(`<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>DNS Cache</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body {
    font-family: system-ui, -apple-system, sans-serif;
    background: #0f172a;
    color: #e2e8f0;
    padding: 2rem;
    max-width: 960px;
    margin: 0 auto;
  }
  h1 {
    font-size: 1.5rem;
    font-weight: 600;
    margin-bottom: 1.5rem;
    color: #38bdf8;
  }
  h1 small { font-size: 0.875rem; color: #64748b; font-weight: 400; }
  h2 {
    font-size: 1.1rem;
    font-weight: 600;
    margin: 1.5rem 0 0.75rem;
    color: #94a3b8;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 1rem;
    margin-bottom: 1.5rem;
  }
  .card {
    background: #1e293b;
    border-radius: 0.75rem;
    padding: 1.25rem;
    border: 1px solid #334155;
  }
  .card .label { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; color: #64748b; margin-bottom: 0.25rem; }
  .card .value { font-size: 1.75rem; font-weight: 700; }
  .card .value.green { color: #4ade80; }
  .card .value.blue { color: #38bdf8; }
  .card .value.yellow { color: #facc15; }
  .card .value.red { color: #f87171; }
  .card .value.purple { color: #a78bfa; }
  .card .value.cyan { color: #22d3ee; }
  .bar-bg {
    background: #334155;
    border-radius: 0.375rem;
    height: 0.625rem;
    margin-top: 0.5rem;
    overflow: hidden;
  }
  .bar-fill {
    height: 100%;
    border-radius: 0.375rem;
    transition: width 2s;
  }
  .health-row {
    display: flex;
    gap: 1.5rem;
    margin-bottom: 1.5rem;
    font-size: 0.875rem;
  }
  .health-row span { color: #94a3b8; }
  .health-row .count { font-weight: 700; color: #e2e8f0; }
  .chart-box {
    background: #1e293b;
    border-radius: 0.75rem;
    padding: 1rem;
    border: 1px solid #334155;
    margin-bottom: 1.5rem;
  }
  .chart-box canvas { display: block; width: 100%; height: 80px; }
  table {
    width: 100%;
    border-collapse: collapse;
    background: #1e293b;
    border-radius: 0.75rem;
    overflow: hidden;
    border: 1px solid #334155;
  }
  th {
    background: #334155;
    color: #94a3b8;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: 0.75rem 1rem;
    text-align: left;
  }
  td {
    padding: 0.625rem 1rem;
    border-top: 1px solid #334155;
    font-size: 0.875rem;
  }
  .hit { color: #4ade80; }
  .ttl { color: #94a3b8; font-size: 0.75rem; }
  .dist { margin-bottom: 1.5rem; }
  .dist-row { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.25rem; font-size: 0.8125rem; }
  .dist-label { width: 4rem; color: #94a3b8; text-align: right; }
  .dist-bar { flex: 1; height: 1rem; background: #334155; border-radius: 0.25rem; overflow: hidden; }
  .dist-fill { height: 100%; border-radius: 0.25rem; min-width: 2px; }
  .dist-count { width: 2.5rem; text-align: right; color: #e2e8f0; }
  .footer { text-align: center; color: #475569; font-size: 0.75rem; margin-top: 2rem; }
</style>
</head>
<body>
<h1>DNS Cache <small>auto-refresh 5s</small></h1>

<div class="grid">
  <div class="card">
    <div class="label">Entry in cache</div>
    <div class="value blue">{{.Cache.Entries}}</div>
    <div class="bar-bg"><div class="bar-fill" style="width:{{printf "%.0f" (percent .Cache.Entries .Cache.MaxEntries)}}%;background:#38bdf8"></div></div>
  </div>
  <div class="card">
    <div class="label">Hit ratio</div>
    <div class="value green">{{printf "%.1f" .Cache.HitRatio}}%</div>
  </div>
  <div class="card">
    <div class="label">QPS (media 60s)</div>
    <div class="value cyan">{{printf "%.1f" .Cache.AvgQPS}}</div>
  </div>
  <div class="card">
    <div class="label">Query totali</div>
    <div class="value blue">{{.Cache.TotalQueries}}</div>
  </div>
  <div class="card">
    <div class="label">Hits</div>
    <div class="value green">{{.Cache.Hits}}</div>
  </div>
  <div class="card">
    <div class="label">Misses</div>
    <div class="value yellow">{{.Cache.Misses}}</div>
  </div>
  <div class="card">
    <div class="label">Stale serves</div>
    <div class="value purple">{{.Cache.StaleServes}}</div>
  </div>
  <div class="card">
    <div class="label">Errori</div>
    <div class="value red">{{.Cache.Errors}}</div>
  </div>
</div>

<div class="health-row">
  <span>Attive: <span class="count" style="color:#4ade80">{{.ActiveEntries}}</span></span>
  <span>Scadute: <span class="count" style="color:#facc15">{{.ExpiredEntries}}</span></span>
  <span>Uptime: <span class="count">{{.Uptime}}</span></span>
</div>

{{if .Cache.QPSHistory}}
<div class="chart-box">
  <div style="display:flex;justify-content:space-between;margin-bottom:0.5rem;font-size:0.75rem;color:#64748b;">
    <span>QPS (ultimi 60s)</span>
    <span>now</span>
  </div>
  <canvas id="qpsChart"></canvas>
</div>
<script>
(function(){
  var data = [{{range $i, $v := .Cache.QPSHistory}}{{if $i}},{{end}}{{$v}}{{end}}];
  var c = document.getElementById("qpsChart");
  c.width = c.clientWidth * 2; c.height = c.clientHeight * 2;
  var ctx = c.getContext("2d");
  ctx.scale(2, 2);
  var w = c.clientWidth, h = c.clientHeight;
  var max = 1, sum = 0;
  for (var i = 0; i < data.length; i++) { if (data[i] > max) max = data[i]; sum += data[i]; }
  var avg = sum / data.length;
  ctx.clearRect(0, 0, w, h);
  ctx.beginPath();
  ctx.strokeStyle = "#22d3ee";
  ctx.lineWidth = 1.5;
  for (var i = 0; i < data.length; i++) {
    var x = (i / (data.length - 1)) * w;
    var y = h - (data[i] / max) * (h - 4) - 2;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }
  ctx.stroke();
  if (avg > 0) {
    var ay = h - (avg / max) * (h - 4) - 2;
    ctx.beginPath();
    ctx.strokeStyle = "rgba(148,163,184,0.3)";
    ctx.lineWidth = 1;
    ctx.setLineDash([3, 3]);
    ctx.moveTo(0, ay); ctx.lineTo(w, ay);
    ctx.stroke();
    ctx.setLineDash([]);
  }
})();
</script>
{{end}}

<h2>Distribuzione per tipo</h2>
<div class="dist">
{{$max := 0}}{{range $t, $c := .QueryTypeDist}}{{if gt $c $max}}{{$max = $c}}{{end}}{{end}}
{{range $type, $count := .QueryTypeDist}}
<div class="dist-row">
  <span class="dist-label">{{$type}}</span>
  <div class="dist-bar"><div class="dist-fill" style="width:{{printf "%.0f" (percent $count $max)}}%;background:{{colorForType $type}}"></div></div>
  <span class="dist-count">{{$count}}</span>
</div>
{{else}}
<div style="color:#64748b;font-size:0.875rem;">Nessuna entry in cache</div>
{{end}}
</div>

<h2>Top domini</h2>
<table>
<thead><tr><th>Dominio</th><th>Tipo</th><th>Hit</th><th>TTL</th></tr></thead>
<tbody>
{{range .TopDomains}}
<tr>
  <td>{{.Name}}</td>
  <td>{{.Type}}</td>
  <td class="hit">{{.HitCount}}</td>
  <td class="ttl">{{.TTL}}s</td>
</tr>
{{else}}
<tr><td colspan="4" style="text-align:center;color:#64748b;">Nessuna entry in cache</td></tr>
{{end}}
</tbody>
</table>

<div class="footer">
  <a href="/api/stats" style="color:#38bdf8;">JSON API</a>
</div>
</body>
</html>`))
