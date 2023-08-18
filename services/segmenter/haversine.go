package segmenter

import (
	"math"
)

const (
	earthRaidusM = 6_371_000 // radius of the earth in kilometers.
)

// Coord represents a geographic coordinate.
type Coord struct {
	Lat float64
	Lon float64
}

// degreesToRadians converts from degrees to radians.
func degreesToRadians(d float64) float64 {
	return d * math.Pi / 180
}

// Distance calculates the shortest path between two coordinates on the surface
// of the Earth. The returned distance is in meters.
func Distance(p1, p2 Point) float64 {
	lat1 := degreesToRadians(p1.Latitude)
	lon1 := degreesToRadians(p1.Longitude)
	lat2 := degreesToRadians(p2.Latitude)
	lon2 := degreesToRadians(p2.Longitude)

	diffLat := lat2 - lat1
	diffLon := lon2 - lon1

	a := math.Pow(math.Sin(diffLat/2), 2) + math.Cos(lat1)*math.Cos(lat2)*math.Pow(math.Sin(diffLon/2), 2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRaidusM * c
}
