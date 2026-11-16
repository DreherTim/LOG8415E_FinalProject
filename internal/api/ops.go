package api

// ops.go — the whitelist of operations.
//
// Clients never send SQL. They send an operation name plus typed parameters.
// Anything not listed here is rejected. The SQL is built only in the Proxy,
// with prepared statements.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-playground/validator/v10"
)

// ---- Parameter structs ----
// Sakila IDs are small positive integers; names are VARCHAR(45).

type GetFilmParams struct {
	FilmID int `json:"film_id" validate:"required,min=1,max=1000000"`
}

type GetActorParams struct {
	ActorID int `json:"actor_id" validate:"required,min=1,max=1000000"`
}

type ListActorFilmsParams struct {
	ActorID int `json:"actor_id" validate:"required,min=1,max=1000000"`
	Limit   int `json:"limit"    validate:"omitempty,min=1,max=50"`
}

type InsertActorParams struct {
	FirstName string `json:"first_name" validate:"required,alpha,max=45"`
	LastName  string `json:"last_name"  validate:"required,alpha,max=45"`
}

type UpdateActorLastNameParams struct {
	ActorID  int    `json:"actor_id"  validate:"required,min=1,max=1000000"`
	LastName string `json:"last_name" validate:"required,alpha,max=45"`
}

type opSpec struct {
	kind      OpKind
	newParams func() any
}

// operations is the complete whitelist.
var operations = map[string]opSpec{
	// reads
	"get_film":         {KindRead, func() any { return &GetFilmParams{} }},
	"get_actor":        {KindRead, func() any { return &GetActorParams{} }},
	"list_actor_films": {KindRead, func() any { return &ListActorFilmsParams{} }},
	// writes
	"insert_actor":           {KindWrite, func() any { return &InsertActorParams{} }},
	"update_actor_last_name": {KindWrite, func() any { return &UpdateActorLastNameParams{} }},
}

var validate = validator.New(validator.WithRequiredStructEnabled())

var ErrUnknownOp = errors.New("unknown operation")

// ParseOp checks that op is whitelisted and that raw matches its schema
// exactly: correct JSON types, no unknown fields, all constraints satisfied.
// It returns the operation's kind and a pointer to the typed params struct.
func ParseOp(op string, raw json.RawMessage) (OpKind, any, error) {
	spec, ok := operations[op]
	if !ok {
		return "", nil, ErrUnknownOp
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	params := spec.newParams()

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields() // {"film_id":1,"extra":"..."} -> rejected
	if err := dec.Decode(params); err != nil {
		return "", nil, fmt.Errorf("invalid params: %w", err)
	}
	if dec.More() {
		return "", nil, errors.New("invalid params: trailing data")
	}
	if err := validate.Struct(params); err != nil {
		return "", nil, fmt.Errorf("invalid params: %w", err)
	}
	return spec.kind, params, nil
}
