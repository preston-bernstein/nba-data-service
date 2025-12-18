package providers

import "time"

// ResolveTimezone returns a location for a tz string, or nil if invalid.
func ResolveTimezone(tz string) *time.Location {
	if tz == "" {
		return nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil
	}
	return loc
}
