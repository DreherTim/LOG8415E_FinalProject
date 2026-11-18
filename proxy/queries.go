package main

// queries.go — the only place SQL exists. Each whitelisted operation maps to
// one fixed, parameterised statement; user values are only ever bound as
// parameters, never concatenated into SQL.

import (
	"context"
	"fmt"

	"github.com/DreherTim/LOG8415E_FinalProject/internal/api"
)

const (
	qGetFilm = `SELECT film_id, title, description, release_year, rating, length
	            FROM film WHERE film_id = ?`
	qGetActor = `SELECT actor_id, first_name, last_name, last_update
	             FROM actor WHERE actor_id = ?`
	qListActorFilms = `SELECT f.film_id, f.title
	                   FROM film f JOIN film_actor fa ON fa.film_id = f.film_id
	                   WHERE fa.actor_id = ? ORDER BY f.title LIMIT ?`
	qInsertActor         = `INSERT INTO actor (first_name, last_name) VALUES (?, ?)`
	qUpdateActorLastName = `UPDATE actor SET last_name = ? WHERE actor_id = ?`
)

const defaultListLimit = 10

type result struct {
	rows         []map[string]any
	rowsAffected *int64
	lastInsertID *int64
}

// execute runs the operation described by the typed params on node n.
func execute(ctx context.Context, n *node, params any) (result, error) {
	switch p := params.(type) {
	case *api.GetFilmParams:
		return query(ctx, n, qGetFilm, p.FilmID)
	case *api.GetActorParams:
		return query(ctx, n, qGetActor, p.ActorID)
	case *api.ListActorFilmsParams:
		limit := p.Limit
		if limit == 0 {
			limit = defaultListLimit
		}
		return query(ctx, n, qListActorFilms, p.ActorID, limit)
	case *api.InsertActorParams:
		return exec(ctx, n, qInsertActor, p.FirstName, p.LastName)
	case *api.UpdateActorLastNameParams:
		return exec(ctx, n, qUpdateActorLastName, p.LastName, p.ActorID)
	default:
		return result{}, fmt.Errorf("no SQL mapping for %T", params)
	}
}

func query(ctx context.Context, n *node, q string, args ...any) (result, error) {
	st, err := n.stmt(ctx, q)
	if err != nil {
		return result{}, err
	}
	rows, err := st.QueryContext(ctx, args...)
	if err != nil {
		return result{}, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return result{}, err
	}
	out := []map[string]any{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return result{}, err
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			if b, ok := vals[i].([]byte); ok {
				row[c] = string(b) // text columns arrive as []byte
			} else {
				row[c] = vals[i]
			}
		}
		out = append(out, row)
	}
	return result{rows: out}, rows.Err()
}

func exec(ctx context.Context, n *node, q string, args ...any) (result, error) {
	st, err := n.stmt(ctx, q)
	if err != nil {
		return result{}, err
	}
	res, err := st.ExecContext(ctx, args...)
	if err != nil {
		return result{}, err
	}
	var r result
	if v, err := res.RowsAffected(); err == nil {
		r.rowsAffected = &v
	}
	if v, err := res.LastInsertId(); err == nil && v != 0 {
		r.lastInsertID = &v
	}
	return r, nil
}
