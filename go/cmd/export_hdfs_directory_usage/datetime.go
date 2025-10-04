package main

import "time"

var _timeFormats = [...]string{
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02",
}

type DateTime struct{ time.Time }

func (t *DateTime) UnmarshalText(text []byte) error {
	var (
		date time.Time
		err  error
	)
	for _, fmt := range _timeFormats {
		if date, err = time.Parse(fmt, string(text)); err == nil {
			t.Time = date
			return nil
		}
	}
	return err
}
