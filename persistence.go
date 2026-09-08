package main

import (
	"database/sql"
	"log"
	"os"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"

	"github.com/miekg/dns"

	"dns-cache/cache"
)

type Persistence struct {
	db     *sql.DB
	dbPath string
}

func NewPersistence(dbPath string) (*Persistence, error) {
	dir := dirOf(dbPath)
	if dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA synchronous=NORMAL`); err != nil {
		return nil, err
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS cache_entries (
			question_name  TEXT    NOT NULL,
			question_type  INTEGER NOT NULL,
			response_data  BLOB   NOT NULL,
			stored_at      INTEGER NOT NULL,
			original_ttl   INTEGER NOT NULL,
			cached_ttl     INTEGER NOT NULL DEFAULT 0,
			hit_count      INTEGER DEFAULT 0,
			last_hit_at    INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (question_name, question_type)
		)
	`); err != nil {
		return nil, err
	}

	if err := addColumnIfMissing(db, "last_hit_at", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return nil, err
	}

	return &Persistence{db: db, dbPath: dbPath}, nil
}

// addColumnIfMissing aggiunge una colonna ai db gia' esistenti: CREATE TABLE
// IF NOT EXISTS non tocca una tabella gia' creata da una versione precedente.
func addColumnIfMissing(db *sql.DB, name, decl string) error {
	rows, err := db.Query(`PRAGMA table_info(cache_entries)`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			colName    string
			colType    string
			notNull    int
			dfltValue  sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &colName, &colType, &notNull, &dfltValue, &primaryKey); err != nil {
			return err
		}
		if colName == name {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	_, err = db.Exec(`ALTER TABLE cache_entries ADD COLUMN ` + name + ` ` + decl)
	return err
}

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return ""
}

func (p *Persistence) Close() error {
	return p.db.Close()
}

func (p *Persistence) LoadAll() ([]*cache.Entry, error) {
	rows, err := p.db.Query(`
		SELECT question_name, question_type, response_data, stored_at, original_ttl, cached_ttl, hit_count, last_hit_at
		FROM cache_entries
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*cache.Entry
	var skipped int
	now := time.Now()

	for rows.Next() {
		var (
			qname     string
			qtype     uint16
			data      []byte
			storedAt  int64
			origTTL   uint32
			cachedTTL int64
			hitCount  uint64
			lastHitAt int64
		)
		if err := rows.Scan(&qname, &qtype, &data, &storedAt, &origTTL, &cachedTTL, &hitCount, &lastHitAt); err != nil {
			log.Printf("[warn] scan row: %v", err)
			continue
		}

		storedTime := time.Unix(storedAt, 0)

		// La scadenza si ricostruisce dal TTL effettivamente applicato dalla
		// cache, non da quello dell'upstream: Set() lo clampa in
		// [ttl_min, ttl_max] e ripartire da origTTL rimetterebbe in circolo
		// entry con una vita diversa da quella che la config consente.
		// cached_ttl e' DEFAULT 0, quindi le righe scritte prima che la
		// colonna esistesse ricadono su origTTL.
		cd := time.Duration(cachedTTL) * time.Second
		if cd <= 0 {
			cd = time.Duration(origTTL) * time.Second
		}
		expiresAt := storedTime.Add(cd)

		// Le entry gia' scadute non si caricano. Riempirebbero la cache (e
		// il tetto per ciclo del refresher) di roba che va comunque
		// rinterrogata alla prima richiesta del client: dopo una notte a
		// macchina spenta sono la maggioranza della tabella.
		if now.After(expiresAt) {
			skipped++
			continue
		}

		// Righe scritte prima che la colonna esistesse: si ripiega su
		// stored_at, che e' l'ora dell'ultimo refresh e non dell'ultima
		// richiesta del client, ma e' il meglio che quelle righe sanno dire.
		if lastHitAt == 0 {
			lastHitAt = storedTime.UnixNano()
		}

		msg := new(dns.Msg)
		if err := msg.Unpack(data); err != nil {
			log.Printf("[warn] unpack cached msg: %v", err)
			continue
		}

		entries = append(entries, &cache.Entry{
			QuestionName: qname,
			QuestionType: qtype,
			Response:     msg,
			StoredAt:     storedTime,
			OriginalTTL:  origTTL,
			CachedTTL:    cd,
			ExpiresAt:    expiresAt,
			HitCount:     hitCount,
			LastHitAt:    lastHitAt,
		})
	}

	if skipped > 0 {
		log.Printf("[persist] skipped %d expired entries", skipped)
	}

	return entries, rows.Err()
}

func (p *Persistence) SaveEntry(e *cache.Entry) error {
	data, err := e.Response.Pack()
	if err != nil {
		return err
	}

	_, err = p.db.Exec(`
		INSERT OR REPLACE INTO cache_entries (question_name, question_type, response_data, stored_at, original_ttl, cached_ttl, hit_count, last_hit_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, e.QuestionName, e.QuestionType, data, e.StoredAt.Unix(), e.OriginalTTL, int64(e.CachedTTL.Seconds()),
		atomic.LoadUint64(&e.HitCount), atomic.LoadInt64(&e.LastHitAt))
	return err
}

func (p *Persistence) SaveBatch(entries []*cache.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO cache_entries (question_name, question_type, response_data, stored_at, original_ttl, cached_ttl, hit_count, last_hit_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, e := range entries {
		data, err := e.Response.Pack()
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(e.QuestionName, e.QuestionType, data, e.StoredAt.Unix(), e.OriginalTTL, int64(e.CachedTTL.Seconds()),
			atomic.LoadUint64(&e.HitCount), atomic.LoadInt64(&e.LastHitAt)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (p *Persistence) DeleteEntry(qname string, qtype uint16) error {
	_, err := p.db.Exec(`DELETE FROM cache_entries WHERE question_name = ? AND question_type = ?`, qname, qtype)
	return err
}

func (p *Persistence) Cleanup(after time.Duration) (int, error) {
	cutoff := time.Now().Add(-after).Unix()
	res, err := p.db.Exec(`DELETE FROM cache_entries WHERE stored_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
