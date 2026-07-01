package main

import (
	"database/sql"
	"log"
	"os"
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
			PRIMARY KEY (question_name, question_type)
		)
	`); err != nil {
		return nil, err
	}

	return &Persistence{db: db, dbPath: dbPath}, nil
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
		SELECT question_name, question_type, response_data, stored_at, original_ttl, cached_ttl, hit_count
		FROM cache_entries
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*cache.Entry
	for rows.Next() {
		var (
			qname     string
			qtype     uint16
			data      []byte
			storedAt  int64
			origTTL   uint32
			cachedTTL int64
			hitCount  uint64
		)
		if err := rows.Scan(&qname, &qtype, &data, &storedAt, &origTTL, &cachedTTL, &hitCount); err != nil {
			log.Printf("[warn] scan row: %v", err)
			continue
		}

		msg := new(dns.Msg)
		if err := msg.Unpack(data); err != nil {
			log.Printf("[warn] unpack cached msg: %v", err)
			continue
		}

		storedTime := time.Unix(storedAt, 0)
		expiresAt := storedTime.Add(time.Duration(origTTL) * time.Second)
		cd := time.Duration(cachedTTL) * time.Second

		entries = append(entries, &cache.Entry{
			QuestionName: qname,
			QuestionType: qtype,
			Response:     msg,
			StoredAt:     storedTime,
			OriginalTTL:  origTTL,
			CachedTTL:    cd,
			ExpiresAt:    expiresAt,
			HitCount:     hitCount,
			LastHitAt:    storedTime,
		})
	}
	return entries, rows.Err()
}

func (p *Persistence) SaveEntry(e *cache.Entry) error {
	data, err := e.Response.Pack()
	if err != nil {
		return err
	}

	_, err = p.db.Exec(`
		INSERT OR REPLACE INTO cache_entries (question_name, question_type, response_data, stored_at, original_ttl, cached_ttl, hit_count)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, e.QuestionName, e.QuestionType, data, e.StoredAt.Unix(), e.OriginalTTL, int64(e.CachedTTL.Seconds()), e.HitCount)
	return err
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
	if n > 0 {
		log.Printf("[cleanup] removed %d old entries from db", n)
	}
	return int(n), nil
}
