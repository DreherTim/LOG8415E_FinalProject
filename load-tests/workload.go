package main

// workload.go — what the benchmark sends.

import "math/rand/v2"

type op struct {
	Name   string         `json:"op"`
	Params map[string]any `json:"params"`
}

// randomWrite: half new actors, half last-name updates of Sakila actors 1-200.
func randomWrite() op {
	if rand.IntN(2) == 0 {
		return op{"insert_actor", map[string]any{"first_name": randomName(), "last_name": randomName()}}
	}
	return op{"update_actor_last_name", map[string]any{"actor_id": rand.IntN(200) + 1, "last_name": randomName()}}
}

// randomRead: 40% film lookups, 30% actor lookups, 30% films-by-actor lists.
func randomRead() op {
	switch r := rand.IntN(10); {
	case r < 4:
		return op{"get_film", map[string]any{"film_id": rand.IntN(1000) + 1}}
	case r < 7:
		return op{"get_actor", map[string]any{"actor_id": rand.IntN(200) + 1}}
	default:
		return op{"list_actor_films", map[string]any{"actor_id": rand.IntN(200) + 1, "limit": 10}}
	}
}

// randomOp: 50/50 mix, used for warm-up.
func randomOp() op {
	if rand.IntN(2) == 0 {
		return randomWrite()
	}
	return randomRead()
}

// randomName returns 8 uppercase letters (the Gatekeeper only accepts letters).
func randomName() string {
	b := make([]byte, 8)
	for i := range b {
		b[i] = byte('A' + rand.IntN(26))
	}
	return string(b)
}
