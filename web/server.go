package web

import (
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
		s.srv.Close()
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

var tmpl = template.Must(template.New("dashboard").Parse(`<!DOCTYPE html>
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
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
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
  .footer { text-align: center; color: #475569; font-size: 0.75rem; margin-top: 2rem; }
</style>
</head>
<body>
<h1>DNS Cache <small>auto-refresh 5s</small></h1>

<div class="grid">
  <div class="card">
    <div class="label">Entry in cache</div>
    <div class="value blue">{{.Cache.Entries}}</div>
  </div>
  <div class="card">
    <div class="label">Hit ratio</div>
    <div class="value green">{{printf "%.1f" .Cache.HitRatio}}%</div>
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
  Uptime: {{.Uptime}} &mdash; <a href="/api/stats" style="color:#38bdf8;">JSON API</a>
</div>
</body>
</html>`))
