package main

import "fmt"
import "net/http"
import "flag"
import "time"
import "strconv"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import "github.com/golang/glog"
import "code.google.com/p/gcfg"

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
	result = make(map[string]string)
	var rows *sql.Rows
	if rows, err = db.Query(query); err == nil {
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			glog.V(2).Info("mysql Query return err with :", err)
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
			if err = rows.Scan(scanArgs...); err != nil {
				glog.V(2).Info("mysql rows scan eturn err with :", err)
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

		glog.V(2).Info("mysql Query return :", result)
	} else {
		glog.V(2).Info("mysql Query return error :", err)
	}

	return result, err
}

func getStatus(host, username, password string, interval time.Duration, sbm int) {

	firstCheck := true
	db, err := sql.Open("mysql", username+":"+password+"@tcp("+host+")/")
	defer db.Close()

	if err != nil {
		glog.V(1).Info("mysql Open return err with :", err)
	}

	for {
		//check if the server is master
		dbMaster := false
		dbSlave := false
		dbRuns := false

		if result, err := getQueryResult(db, "show variables like 'read_only';"); err == nil {
			dbRuns = true
			if result["Value"] == "OFF" {
				dbMaster = true
			}
		}

		if !dbMaster {
			if result, err := getQueryResult(db, "show slave status;"); err == nil {
				dbRuns = true
				if v, ok := result["Seconds_Behind_Master"]; ok {
					if sbmVal, ok := strconv.Atoi(v); ok == nil && sbmVal <= sbm {
						dbSlave = true
					}
				}
			} else {
				glog.V(2).Info("Query failed with error ", err)
			}
		}

		if gDBMaster != dbMaster {
			glog.V(1).Info("DBMaster status change to ", dbMaster)
			gDBMaster = dbMaster
		}

		if gDBSlave != dbSlave {
			glog.V(1).Info("DBSlave status change to ", dbSlave)
			gDBSlave = dbSlave
		}

		if gDBRuns != dbRuns {
			glog.V(1).Info("DBRuns status change to ", dbRuns)
			gDBRuns = dbRuns
		}

		if firstCheck {
			hasStatus <- true
			firstCheck = false
		}

		glog.Flush()
		time.Sleep(interval * time.Second)
	}
}

type SETTINGS struct {
	Username string
	Password string
	Host     string
	Interval int
	Sbm      int
	Port     string
}

type CFG struct {
	Settings SETTINGS
}

func main() {
	cfg := CFG{
		Settings: SETTINGS{
			Username: "mha",
			Password: "",
			Host:     "locahost:3306",
			Interval: 1,
			Sbm:      150,
			Port:     ":3300",
		},
	}

	cfgFile := flag.String("cfg", "./mysql_ms_checker.cfg", "configuration file for mysql master/slaver checher")
	if err := gcfg.ReadFileInto(&cfg, *cfgFile); err != nil {
		glog.V(0).Info("Failed to parse gcfg data: %s", err)
	}
	username := flag.String("u", cfg.Settings.Username, "user name")
	password := flag.String("p", cfg.Settings.Password, "password")
	host := flag.String("h", cfg.Settings.Host, "host")
	interval := flag.Int("i", cfg.Settings.Interval, "interval of the check")
	sbm := flag.Int("sbm", cfg.Settings.Sbm, "Second behind master threshold")

	flag.Parse()

	glog.V(1).Info("mysql_ms_checker start")
	//Goroutines
	go getStatus(*host, *username, *password, time.Duration(*interval), *sbm)
	<-hasStatus

	glog.V(1).Info("mysql_ms_checker finish getStatus")

	// your http.Handle calls here
	http.Handle("/checkMaster", http.HandlerFunc(checkMaster))
	http.Handle("/checkSlave", http.HandlerFunc(checkSlave))
	http.Handle("/checkLiveSlave", http.HandlerFunc(checkLiveSlave))

	err := http.ListenAndServe(cfg.Settings.Port, nil)
	if err != nil {
		glog.Fatalf("ListenAndServe: %s", err)
	}
	glog.V(1).Info("mysql_ms_checker end")
}
