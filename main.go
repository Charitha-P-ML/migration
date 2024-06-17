package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/beltran/gohive"
)

// wasbs://archive-4eqym6ej-profile@apmmanagerstorage.blob.core.windows.net/apmmgr-profile/log-4eqym6ej/_tag_projectName=proj1/_tag_appName=app1/_plugin=linux/_documentType=linux-syslog

// s3a://archive-kpfnrcaf-profile-apm/apmmgr-profile/log-4eqym6ej/_tag_projectName=proj1/_tag_appName=app1/_plugin=linux/_documentType=linux-syslog

var s3BucketRegex *regexp.Regexp = regexp.MustCompile(`s3a://archive-[^-]+-(profile-apm|profile)/`)

func getHiveTables(con *gohive.Connection) (hiveTableNames []string) {

	cursor := con.Cursor()

	defer cursor.Close()

	ctx := context.Background()

	showTablesSQL := "SHOW TABLES"

	cursor.Exec(ctx, showTablesSQL)
	if cursor.Err != nil {
		log.Fatalf("failed to get hive tables: %s", cursor.Err.Error())
		panic(cursor.Err)
	}

	var tableName string

	for cursor.HasMore(ctx) {

		cursor.FetchOne(ctx, &tableName)
		if cursor.Err != nil {
			log.Fatalf("failed to get hive tables: %s", cursor.Err.Error())
			panic(cursor.Err)
		}

		hiveTableNames = append(hiveTableNames, tableName)
	}

	return
}

func getPartnsOfHiveTable(con *gohive.Connection, hiveTableName string) (hiveTablePartns []string) {

	cursor := con.Cursor()

	defer cursor.Close()

	ctx := context.Background()

	showPartnsSQL := fmt.Sprintf("SHOW PARTITIONS %s", hiveTableName)

	cursor.Exec(ctx, showPartnsSQL)
	if cursor.Err != nil {
		if strings.Contains(cursor.Err.Error(), "is not a partitioned table") {
			return
		}
		log.Fatalf("failed to get hive table %s's partitions: %s", hiveTableName, cursor.Err.Error())
		panic(cursor.Err)
	}

	var tablePartn string

	for cursor.HasMore(ctx) {

		cursor.FetchOne(ctx, &tablePartn)
		if cursor.Err != nil {
			log.Fatalf("failed to get hive table %s's partitions: %s", hiveTableName, cursor.Err.Error())
			panic(cursor.Err)
		}

		hiveTablePartns = append(hiveTablePartns, tablePartn)
	}

	return
}

func getHivePartnLocation(con *gohive.Connection, hiveTableName string, hiveTablePartn string) string {

	cursor := con.Cursor()

	defer cursor.Close()

	ctx := context.Background()

	descHivePartnSQL := fmt.Sprintf("DESCRIBE FORMATTED %s PARTITION(%s)", hiveTableName, strings.ReplaceAll(hiveTablePartn, "/", ", "))

	cursor.Exec(ctx, descHivePartnSQL)
	if cursor.Err != nil {
		log.Fatalf("failed to describe hive table %s's partition %s: %s", hiveTableName, hiveTablePartn, cursor.Err.Error())
		panic(cursor.Err)
	}

	var col1 string
	var col2 string
	var col3 string

	for cursor.HasMore(ctx) {

		cursor.FetchOne(ctx, &col1, &col2, &col3)
		if cursor.Err != nil {
			log.Fatalf("failed to describe hive table %s's partition %s: %s", hiveTableName, hiveTablePartn, cursor.Err.Error())
			panic(cursor.Err)
		}

		if strings.HasPrefix(col1, "Location") {
			return col2
		}
	}

	return ""
}

func alterHivePartnLocation(con *gohive.Connection, hiveTableName string, hiveTablePartn string, newLocation string) {

	cursor := con.Cursor()

	defer cursor.Close()

	ctx := context.Background()

	alterPartnSQL := fmt.Sprintf("ALTER TABLE %s PARTITION(%s) SET LOCATION '%s'", hiveTableName, strings.ReplaceAll(hiveTablePartn, "/", ", "), newLocation)

	cursor.Exec(ctx, alterPartnSQL)
	if cursor.Err != nil {
		log.Fatalf("failed to alter hive table %s's partition %s location: %s", hiveTableName, hiveTablePartn, cursor.Err.Error())
		panic(cursor.Err)
	}
}

func getHiveTableLocation(con *gohive.Connection, hiveTableName string) string {

	cursor := con.Cursor()

	defer cursor.Close()

	ctx := context.Background()

	descTableSQL := fmt.Sprintf("DESCRIBE FORMATTED %s", hiveTableName)

	cursor.Exec(ctx, descTableSQL)
	if cursor.Err != nil {
		log.Fatalf("failed to describe hive table %s: %s", hiveTableName, cursor.Err.Error())
		panic(cursor.Err)
	}

	var col1 string
	var col2 string
	var col3 string

	for cursor.HasMore(ctx) {

		cursor.FetchOne(ctx, &col1, &col2, &col3)
		if cursor.Err != nil {
			log.Fatalf("failed to describe hive table %s: %s", hiveTableName, cursor.Err.Error())
			panic(cursor.Err)
		}

		if strings.HasPrefix(col1, "Location") {
			return col2
		}
	}

	return ""
}

func alterHiveTableLocation(con *gohive.Connection, hiveTableName string, newLocation string) {

	cursor := con.Cursor()

	defer cursor.Close()

	ctx := context.Background()

	alterTableSQL := fmt.Sprintf("ALTER TABLE %s SET LOCATION '%s'", hiveTableName, newLocation)

	cursor.Exec(ctx, alterTableSQL)
	if cursor.Err != nil {
		log.Fatalf("failed to alter hive table %s's location: %s", hiveTableName, cursor.Err.Error())
		panic(cursor.Err)
	}
}

// TO RUN the SCRIPT use BELOW
// go run main.go --storage-account apmmanagerstorage

func main() {

	hiveUsername := flag.String("username", "root", "Username to use to connect with Hive")
	hiveAuth := flag.String("auth", "NONE", "Authentication type to use to connect with Hive")
	hiveHost := flag.String("host", "archival-hive-server", "Hive server host")
	hivePort := flag.Int("port", 10000, "Hive server port")
	storageAccount := flag.String("storage-account", "apmmanagerstorage", "azure storage account")
	flag.Parse()

	config := gohive.NewConnectConfiguration()
	config.Username = *hiveUsername

	con, err := gohive.Connect(*hiveHost, *hivePort, *hiveAuth, config)
	if err != nil {
		log.Fatalf("failed to connect to hive: %s", err.Error())
		panic(err)
	}

	hiveTableNames := getHiveTables(con)

	for _, tableName := range hiveTableNames {

		// Start altering the location of this hive table
		tableLoc := getHiveTableLocation(con, tableName)
		var newTableLoc string

		tableBucket := s3BucketRegex.FindString(tableLoc)

		if tableBucket != "" {
			newTableBucket := fmt.Sprintf("wasbs://%s@%s.blob.core.windows.net/", strings.TrimPrefix(strings.TrimSuffix(tableBucket, "/"), "s3a://"), *storageAccount)
			newTableLoc = strings.Replace(tableLoc, tableBucket, newTableBucket, 1)

			log.Printf("table %s is located at %s. Location will be altered to %s", tableName, tableLoc, newTableLoc)
			// TODO: uncomment below in final run
			alterHiveTableLocation(con, tableName, newTableLoc)
			log.Printf("successfully altered table %s's location", tableName)
		} else {
			log.Printf("table %s is located at %s", tableName, tableLoc)
		}

		// // Start altering the locations of this hive table's partitions
		// hiveTablePartns := getPartnsOfHiveTable(con, tableName)

		// for _, partn := range hiveTablePartns {

		// 	partnLoc := getHivePartnLocation(con, tableName, partn)
		// 	var newPartnLoc string

		// 	partnBucket := s3BucketRegex.FindString(partnLoc)

		// 	if partnBucket == "" {
		// 		// No match for bucket name. It may be altered already. Skip this partition
		// 		continue
		// 	}

		// 	newPartnBucket := fmt.Sprintf("wasbs://%s@%s.blob.core.windows.net/", strings.TrimPrefix(strings.TrimSuffix(partnBucket, "/"), "s3a://"), *storageAccount)
		// 	newPartnLoc = strings.Replace(partnLoc, partnBucket, newPartnBucket, 1)

		// 	log.Printf("table %s partition %s is located at %s. Location will be altered to %s", tableName, partn, partnLoc, newPartnLoc)
		// 	// TODO: uncomment below in final run
		// 	// alterHivePartnLocation(con, tableName, partn, newPartnLoc)
		// 	log.Printf("successfully altered table %s partition %s's location", tableName, partn)
		// }
	}
}
