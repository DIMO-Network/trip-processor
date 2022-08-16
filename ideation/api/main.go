package main

import (
	"database/sql"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	_ "github.com/lib/pq"
)

var (
	db *sql.DB
)

type DevicePointInTime struct {
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lon"`
	Order     int     `json:"order"`
	Timestamp string  `json:"timestamp"`
	Speed     float64 `json:"speed"`
	Odometer  float64 `json:"odometer"`
}

type DeviceMetaData struct {
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lon"`
	DeviceID  string  `json:"deviceID"`
}

func init() {
	var err error
	db, err = sql.Open("postgres", "host=localhost port=5433 user=postgres password=postgres dbname=pg_db sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	defer db.Close()
	app := fiber.New()
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
	}))

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello, World!")
	})
	app.Get("/alldevices", getAllDeviceIDsHandler)
	app.Get("/device/:deviceID", getUserTrips)

	app.Listen(":8000")
}

func getAllDeviceIDsHandler(c *fiber.Ctx) error {

	deviceIDs := make([]DeviceMetaData, 0)
	userSQL := "SELECT devicekey, ST_Y(ST_Centroid(ST_UNION(geom))) lat, ST_X(ST_Centroid(ST_UNION(geom))) lon FROM public.trips GROUP BY devicekey;"
	rows, err := db.Query(userSQL)
	if err != nil {
		return err
	}

	for rows.Next() {
		var dMD DeviceMetaData
		err := rows.Scan(&dMD.DeviceID, &dMD.Latitude, &dMD.Longitude)
		if err != nil {
			return err
		}
		deviceIDs = append(deviceIDs, dMD)
	}

	return c.JSON(deviceIDs)
}

func getUserTrips(c *fiber.Ctx) error {
	deviceID := c.Params("deviceID")
	deviceInfo := make([]DevicePointInTime, 0)
	userSQL := "SELECT ST_X(geom) lon, ST_Y(geom) lat, pointnum as order, coord_timestamp as timestamp, speed, odometer FROM public.trips WHERE devicekey = $1;"
	rows, err := db.Query(userSQL, deviceID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var deviceDataPoint DevicePointInTime
		err := rows.Scan(&deviceDataPoint.Longitude, &deviceDataPoint.Latitude, &deviceDataPoint.Order, &deviceDataPoint.Timestamp, &deviceDataPoint.Speed, &deviceDataPoint.Odometer)
		if err != nil {
			return err
		}
		deviceInfo = append(deviceInfo, deviceDataPoint)
	}

	return c.JSON(deviceInfo)
}
