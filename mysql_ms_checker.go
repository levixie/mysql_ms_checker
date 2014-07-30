package main

import "fmt"
import "log"
import "net/http"
import "flag"
import "time"
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

func getStatus(username, password string, interval time.Duration) {

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

		rows, err := db.Query("show variables like 'read_only';")
		if err == nil {
			dbRuns = true
			if err := rows.Err(); err == nil {
				for rows.Next() {
					var name, value string
					if err := rows.Scan(&name, &value); err != nil {
						log.Println("mysql Open return err with :", err)
					} else {
						log.Printf("mysql Open return with name=%v value=%v\n", name, value)
						if value == "OFF" {
							dbMaster = true
							break
						}
					}

				}
			}
			rows.Close()

		} else {
			log.Println("mysql Open return err with :", err)
		}

		if !dbMaster {
			//chech the if slave avaliable to use
			rows, err = db.Query("SHOW SLAVE STATUS;")
			if err == nil {
				dbRuns = true
				if err := rows.Err(); err == nil {
					for rows.Next() {
						var name, value string
						if err := rows.Scan(&name, &value); err != nil {
							log.Println("mysql Open return err with :", err)
						} else {
							log.Printf("mysql Open return with name=%v value=%v\n", name, value)
							if value == "OFF" {
								dbSlave = false
								break
							}
						}

					}
				}
				rows.Close()
			} else {
				log.Println("mysql Open return err with :", err)
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

	log.Println("mysql_ms_checker start")
	//Goroutines
	go getStatus(*username, *password, time.Duration(*interval))
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
