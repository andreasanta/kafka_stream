package model

import (
	"encoding/json"
	"time"
)

/**

Sample record:

	T1
	1
	CP-1
	B1
	20/05/2020
	<today date>
	N

*/

type Trade struct {
	ID           string    `json:"id" gorm:"primaryKey"`
	Version      uint      `json:"version" gorm:"primaryKey"`
	CounterParty string    `json:"counter_party"`
	BookId       string    `json:"book"`
	Expired      bool      `json:"expired"`
	MaturesAt    time.Time `json:"matures_at"`
	CreatedAt    time.Time `json:"created_at"`

	encoded []byte
	err     error
}

func (t *Trade) ensureEncoded() {
	if t.encoded == nil && t.err == nil {
		t.encoded, t.err = json.Marshal(t)
	}
}

func (t *Trade) Length() int {
	t.ensureEncoded()
	return len(t.encoded)
}

func (t *Trade) Encode() ([]byte, error) {
	t.ensureEncoded()
	return t.encoded, t.err
}
