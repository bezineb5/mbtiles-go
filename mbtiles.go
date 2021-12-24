package mbtiles

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// MBtiles provides a basic handle for an mbtiles file.
type MBtiles struct {
	filename  string
	pool      *sql.DB
	tileStmt  *sql.Stmt
	format    TileFormat
	timestamp time.Time
	tilesize  uint32
}

// FindMBtiles recursively finds all mbtiles files within a given path.
func FindMBtiles(path string) ([]string, error) {
	var filenames []string
	err := filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Ignore any that have an associated -journal file; these are incomplete
		if _, err := os.Stat(p + "-journal"); err == nil {
			return nil
		}
		if ext := filepath.Ext(p); ext == ".mbtiles" {
			filenames = append(filenames, p)

		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return filenames, err
}

// Open opens an MBtiles file for reading, and validates that it has the correct
// structure.
func Open(path string) (*MBtiles, error) {
	// try to open file; fail fast if it doesn't exist
	stat, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("path does not exist: %q", path)
		}
		return nil, err
	}

	// there must not be a corresponding *-journal file (tileset is still being created)
	if _, err := os.Stat(path + "-journal"); err == nil {
		return nil, fmt.Errorf("refusing to open mbtiles file with associated -journal file (incomplete tileset)")
	}

	pool, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	db := &MBtiles{
		filename:  path,
		pool:      pool,
		timestamp: stat.ModTime().Round(time.Second),
	}

	con, err := db.getConnection(context.TODO())
	defer db.closeConnection(con)
	if err != nil {
		return nil, err
	}

	err = validateRequiredTables(con)
	if err != nil {
		return nil, err
	}

	format, tilesize, err := getTileFormatAndSize(con)
	if err != nil {
		return nil, err
	}

	db.format = format
	db.tilesize = tilesize

	db.tileStmt, err = con.Prepare("select tile_data from tiles where zoom_level = ? and tile_column = ? and tile_row = ?")
	if err != nil {
		return nil, err
	}

	db.tileStmt, err = con.Prepare("select tile_data from tiles where zoom_level = ? and tile_column = ? and tile_row = ?")
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes a MBtiles file
func (db *MBtiles) Close() {
	if db.tileStmt != nil {
		db.tileStmt.Close()
	}
	if db.pool != nil {
		db.pool.Close()
	}
}

// ReadTile reads a tile for z, x, y into the provided *[]byte.
// data will be nil if the tile does not exist in the database
func (db *MBtiles) ReadTile(z int64, x int64, y int64, data *[]byte) error {
	if db == nil || db.tileStmt == nil {
		return errors.New("cannot read tile from closed mbtiles database")
	}

	err := db.tileStmt.QueryRow(z, x, y).Scan(data)
	if err != nil {
		if err == sql.ErrNoRows {
			*data = nil // If this tile does not exist in the database, return empty bytes
			return nil
		}
		return err
	}
	return nil
}

// ReadMetadata reads the metadata table into a map, casting their values into
// the appropriate type
func (db *MBtiles) ReadMetadata() (map[string]interface{}, error) {
	if db == nil || db.pool == nil {
		return nil, errors.New("cannot read tile from closed mbtiles database")
	}

	con, err := db.getConnection(context.TODO())
	defer db.closeConnection(con)
	if err != nil {
		return nil, err
	}

	var (
		key   string
		value string
	)
	metadata := make(map[string]interface{})

	rows, err := con.Query("select * from metadata where value is not ''")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		rows.Scan(&key, &value)

		switch key {
		case "maxzoom", "minzoom":
			metadata[key], err = strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("cannot read metadata item %s: %v", key, err)
			}
		case "bounds", "center":
			metadata[key], err = parseFloats(value)
			if err != nil {
				return nil, fmt.Errorf("cannot read metadata item %s: %v", key, err)
			}
		case "json":
			err = json.Unmarshal([]byte(value), &metadata)
			if err != nil {
				return nil, fmt.Errorf("unable to parse JSON metadata item: %v", err)
			}
		default:
			metadata[key] = value
		}
	}

	// Supplement missing values by inferring from available data
	_, hasMinZoom := metadata["minzoom"]
	_, hasMaxZoom := metadata["maxzoom"]
	if !(hasMinZoom && hasMaxZoom) {
		var minZoom, maxZoom int
		err := con.QueryRow("select min(zoom_level), max(zoom_level) from tiles").Scan(&minZoom, &maxZoom)
		if err != nil {
			return metadata, nil
		}
		metadata["minzoom"] = minZoom
		metadata["maxzoom"] = maxZoom
	}
	return metadata, nil
}

func (db *MBtiles) GetFilename() string {
	return db.filename
}

// GetTileFormat returns the TileFormat of the mbtiles file.
func (db *MBtiles) GetTileFormat() TileFormat {
	return db.format
}

// GetTileSize returns the tile size in pixels of the mbtiles file, if detected.
// Returns 0 if tile size is not detected.
func (db *MBtiles) GetTileSize() uint32 {
	return db.tilesize
}

// Timestamp returns the time stamp of the mbtiles file.
func (db *MBtiles) GetTimestamp() time.Time {
	return db.timestamp
}

// getConnection gets a sqlite.Conn from an open connection pool.
// closeConnection(con) must be called to release the connection.
func (db *MBtiles) getConnection(ctx context.Context) (*sql.DB, error) {
	/*con := db.pool.Get(ctx)
	if con == nil {
		return nil, errors.New("connection could not be opened")
	}
	return con, nil*/
	return db.pool, nil
}

// closeConnection closes an open sqlite.Conn and returns it to the pool.
func (db *MBtiles) closeConnection(con *sql.DB) {
	/*if con != nil {
		db.pool.Put(con)
	}*/
}

// validateRequiredTables checks that both 'tiles' and 'metadata' tables are
// present in the database
func validateRequiredTables(con *sql.DB) error {
	var tableCount int
	err := con.QueryRow("SELECT count(*) FROM sqlite_master WHERE name in ('tiles', 'metadata')").Scan(&tableCount)
	if err != nil {
		return err
	}

	if tableCount < 2 {
		return errors.New("missing one or more required tables: tiles, metadata")
	}

	return nil
}

// getTileFormat reads the first 8 bytes of the first tile in the database.
// See TileFormat for list of supported tile formats.
func getTileFormat(con *sql.DB) (TileFormat, error) {
	magicWord := make([]byte, 8)
	err := con.QueryRow("select tile_data from tiles limit 1").Scan(&magicWord)

	if err != nil {
		return UNKNOWN, err
	}

	format, err := detectTileFormat(magicWord)
	if err != nil {
		return UNKNOWN, err
	}

	// GZIP masks PBF, which is only expected type for tiles in GZIP format
	if format == GZIP {
		format = PBF
	}

	return format, nil
}

// getTileFormatAndSize reads the first tile in the database to detect the tile
// format and if PNG also the size.
// See TileFormat for list of supported tile formats.
func getTileFormatAndSize(con *sql.DB) (TileFormat, uint32, error) {
	var tilesize uint32 = 0 // not detected for all formats

	var tileData []byte
	err := con.QueryRow("select tile_data from tiles limit 1").Scan(&tileData)

	if err != nil {
		return UNKNOWN, tilesize, err
	}

	format, err := detectTileFormat(tileData)
	if err != nil {
		return UNKNOWN, tilesize, err
	}

	// GZIP masks PBF, which is only expected type for tiles in GZIP format
	if format == GZIP {
		format = PBF
	}

	tilesize, err = detectTileSize(format, tileData)
	if err != nil {
		return format, tilesize, err
	}

	return format, tilesize, nil
}

// parseFloats converts a commma-delimited string of floats to a slice of
// float64 and returns it and the first error that was encountered.
// Example: "1.5,2.1" => [1.5, 2.1]
func parseFloats(str string) ([]float64, error) {
	split := strings.Split(str, ",")
	var out []float64
	for _, v := range split {
		value, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return out, fmt.Errorf("could not parse %q to floats: %v", str, err)
		}
		out = append(out, value)
	}
	return out, nil
}
