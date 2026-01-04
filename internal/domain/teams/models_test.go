package teams

import (
	"reflect"
	"testing"
)

func TestTeamJSONTags(t *testing.T) {
	type fieldCheck struct {
		name string
		tag  string
	}
	teamType := reflect.TypeOf(Team{})
	fields := []fieldCheck{
		{"ID", "id"},
		{"Name", "name"},
		{"FullName", "fullName"},
		{"Abbreviation", "abbreviation"},
		{"City", "city"},
		{"Conference", "conference"},
		{"Division", "division"},
	}
	for _, fc := range fields {
		f, ok := teamType.FieldByName(fc.name)
		if !ok {
			t.Fatalf("missing field %s", fc.name)
		}
		if tag := f.Tag.Get("json"); tag != fc.tag {
			t.Fatalf("field %s expected tag %s, got %s", fc.name, fc.tag, tag)
		}
	}
}
