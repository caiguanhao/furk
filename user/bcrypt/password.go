package bcrypt

import (
	"database/sql/driver"
	"encoding/json"

	"golang.org/x/crypto/bcrypt"
)

type (
	Password struct {
		HashedPassword
		Password string `validate:"omitempty,gte=6,lte=72"`
	}
)

func (p *Password) MustUpdate(password string, optionalCost ...int) {
	if err := p.Update(password, optionalCost...); err != nil {
		panic(err)
	}
}

func (p *Password) Update(password string, optionalCost ...int) error {
	if err := p.HashedPassword.Update(password, optionalCost...); err != nil {
		return err
	}
	p.Password = password
	return nil
}

func (p Password) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Password)
}

func (p *Password) UnmarshalJSON(t []byte) error {
	var value string
	if err := json.Unmarshal(t, &value); err != nil {
		return err
	}
	return p.Update(value)
}

func (p *Password) Scan(src interface{}) error {
	p.Password = ""
	return p.HashedPassword.Scan(src)
}

type (
	HashedPassword struct {
		Hashed string
	}
)

func (p HashedPassword) String() string {
	return p.Hashed
}

func (p *HashedPassword) MustUpdate(password string, optionalCost ...int) {
	if err := p.Update(password, optionalCost...); err != nil {
		panic(err)
	}
}

func (p *HashedPassword) Update(password string, optionalCost ...int) error {
	if password == "" {
		p.Hashed = ""
		return nil
	}
	cost := bcrypt.DefaultCost
	if len(optionalCost) > 0 {
		cost = optionalCost[0]
	}
	b, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return err
	}
	p.Hashed = string(b)
	return nil
}

func (p HashedPassword) Equal(password string) bool {
	if p.Hashed == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(p.Hashed), []byte(password)) == nil
}

func (p *HashedPassword) Scan(src interface{}) error {
	if value, ok := src.(string); ok {
		p.Hashed = value
	}
	return nil
}

func (p HashedPassword) Value() (driver.Value, error) {
	return p.Hashed, nil
}
