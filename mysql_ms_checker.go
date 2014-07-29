package main

import "fmt"
import "log"
import "net/http"
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

func getStatus() {
	db, err := sql.Open("mysql", "mha:*@/")
	defer db.Close()

	if err != nil {
		log.Println("mysql Open return with :", err)
	}

	//check if the server is master
	dbMaster := false
	rows, err := db.Query("show variables like 'read_only';")
	defer rows.Close()

	if err := rows.Err(); err == nil {
		for rows.Next() {
			var variable string
			if err := rows.Scan(&variable); err == nil {
				if variable == "OFF" {
					dbMaster = true
					break
				}
			}
		}
	}

	gDBMaster = dbMaster

	hasStatus <- true
}

func main() {

	log.Println("mysql_ms_checker start")
	//Goroutines
	go getStatus()
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
