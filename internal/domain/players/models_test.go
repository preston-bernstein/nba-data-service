package players

import (
	"reflect"
	"testing"
)

func TestPlayerJSONTags(t *testing.T) {
	type fieldCheck struct {
		name string
		tag  string
	}
	playerType := reflect.TypeOf(Player{})
	fields := []fieldCheck{
		{"ID", "id"},
		{"FirstName", "firstName"},
		{"LastName", "lastName"},
		{"Position", "position"},
		{"HeightFeet", "heightFeet"},
		{"HeightInches", "heightInches"},
		{"WeightPounds", "weightPounds"},
		{"Team", "team"},
		{"Meta", "meta"},
	}
	for _, fc := range fields {
		f, ok := playerType.FieldByName(fc.name)
		if !ok {
			t.Fatalf("missing field %s", fc.name)
		}
		if tag := f.Tag.Get("json"); tag != fc.tag {
			t.Fatalf("field %s expected tag %s, got %s", fc.name, fc.tag, tag)
		}
	}
}
