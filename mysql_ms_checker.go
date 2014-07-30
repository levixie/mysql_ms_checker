package main

import "fmt"
import "log"
import "net/http"
import "flag"
import "time"
import "strconv"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

var gDBRuns bool = false

var gDBMaster bool = false

var gDBSlave bool = false

var hasStatus = make(chan bool)

func checkMaster(w http.ResponseWriter, req *http.Request) {
	if gDBMaster {
		fmt.Fprint(w, "MySQL is running as master.\r\n")
	} else {
		http.Error(w, "MySQL is not running as master", http.StatusServiceUnavailable)
	}
}

func checkSlave(w http.ResponseWriter, req *http.Request) {
	if gDBSlave {
		fmt.Fprint(w, "MySQL is running as slave.\r\n")
	} else {
		http.Error(w, "MySQL is not running as slave", http.StatusServiceUnavailable)
	}
}

func checkLiveSlave(w http.ResponseWriter, req *http.Request) {
	if gDBRuns {
		fmt.Fprint(w, "MySQL is running as live slave.\r\n")
	} else {
		http.Error(w, "MySQL is not running as live slave", http.StatusServiceUnavailable)
	}
}

func getQueryResult(db *sql.DB, query string) (result map[string]string, err error) {
	//var result map[string]string
	rows, err := db.Query(query)
	defer rows.Close()

	if err == nil {
		columns, err := rows.Columns()
		if err != nil {
			log.Println("mysql Query return err with :", err)
			return result, err
		}

		// Make a slice for the values
		values := make([]sql.RawBytes, len(columns))

		// rows.Scan wants '[]interface{}' as an argument, so we must copy the
		// references into such a slice
		// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// Fetch rows
		for rows.Next() {
			// get RawBytes from data
			err = rows.Scan(scanArgs...)
			if err != nil {
				log.Println("mysql Query return err with :", err)
				return result, err
			}
		}

		// Now do something with the data.
		// Here we just print each column as a string.
		var value string
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			result[columns[i]] = value
		}
	}
	return result, err
}

func getStatus(username, password string, interval time.Duration, sbm int) {

	firstCheck := true
	db, err := sql.Open("mysql", username+":"+password+"@/")
	defer db.Close()

	if err != nil {
		log.Println("mysql Open return err with :", err)
	}

	for {
		//check if the server is master
		dbMaster := false
		dbSlave := false
		dbRuns := false

		if result, err := getQueryResult(db, "show variables like 'read_only';"); err == nil {
			dbRuns = true
			if result["read_only"] == "OFF" {
				dbMaster = true
			}
		}

		if !dbMaster {
			if result, err := getQueryResult(db, "show slave status;"); err == nil {
				dbRuns = true
				if v, ok := result["Seconds_Behind_Master"]; !ok {
					if sbmVal, ok := strconv.Atoi(v); ok != nil && sbmVal <= sbm {
						dbSlave = true
					}
				}
			}
		}

		gDBMaster = dbMaster
		gDBSlave = dbSlave
		gDBRuns = dbRuns

		if firstCheck {
			hasStatus <- true
			firstCheck = false
		}
		time.Sleep(interval * time.Second)
	}
}

func main() {
	username := flag.String("u", "", "user name")
	password := flag.String("p", "", "password")
	interval := flag.Int64("i", 1, "interval of the check")
	sbm := flag.Int("sbm", 150, "Second behind master threshold")

	log.Println("mysql_ms_checker start")
	//Goroutines
	go getStatus(*username, *password, time.Duration(*interval), *sbm)
	<-hasStatus

	log.Println("mysql_ms_checker finish getStatus")

	// your http.Handle calls here
	http.Handle("/checkMaster", http.HandlerFunc(checkMaster))
	http.Handle("/checkSlave", http.HandlerFunc(checkSlave))
	http.Handle("/checkLiveSlave", http.HandlerFunc(checkLiveSlave))

	err := http.ListenAndServe(":3000", nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err)
	}
	log.Println("mysql_ms_checker end")
}
