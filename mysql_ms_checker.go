package main

import "fmt"
import "net/http"
import "flag"
import "time"
import "sync"
import "strconv"
import "database/sql"
import "github.com/go-sql-driver/mysql"
import "github.com/golang/glog"
import "code.google.com/p/gcfg"

type SETTINGS struct {
	Username      string
	Password      string
	Host          string
	Interval      int
	Sbm           int
	Port          string
	MaxDelayAllow int
}

type CFG struct {
	Settings SETTINGS
}

var cfg CFG

var gDBRuns bool = false

var gDBMaster bool = false

var gDBSlave bool = false

var gLastCheckTime time.Time

var gDelayAllow time.Duration

func checkMaster(w http.ResponseWriter, req *http.Request) {
	if gLastCheckTime.Add(gDelayAllow).Before(time.Now()) {
		http.Error(w, "MySQL is not running as master", http.StatusServiceUnavailable)
		glog.V(1).Info("max delay excceed")
	}

	if gDBMaster {
		fmt.Fprint(w, "MySQL is running as master.\r\n")
	} else {
		http.Error(w, "MySQL is not running as master", http.StatusServiceUnavailable)
	}
}

func checkSlave(w http.ResponseWriter, req *http.Request) {
	if gLastCheckTime.Add(gDelayAllow).Before(time.Now()) {
		http.Error(w, "MySQL is not running as master", http.StatusServiceUnavailable)
		glog.V(1).Info("max delay excceed")
	}

	if gDBSlave {
		fmt.Fprint(w, "MySQL is running as slave.\r\n")
	} else {
		http.Error(w, "MySQL is not running as slave", http.StatusServiceUnavailable)
	}
}

func checkLiveSlave(w http.ResponseWriter, req *http.Request) {
	if gLastCheckTime.Add(gDelayAllow).Before(time.Now()) {
		http.Error(w, "MySQL is not running as master", http.StatusServiceUnavailable)
		glog.V(1).Info("max delay excceed")
	}

	if gDBRuns {
		fmt.Fprint(w, "MySQL is running as live slave.\r\n")
	} else {
		http.Error(w, "MySQL is not running as live slave", http.StatusServiceUnavailable)
	}
}

func getQueryResult(db *sql.DB, query string) (result map[string]string, err error) {
	result = make(map[string]string)
	var rows *sql.Rows

	glog.V(2).Info("start getQueryResult ", query)
	glog.V(2).Info("start query ", query)
	if rows, err = db.Query(query); err == nil {
		defer rows.Close()

		glog.V(2).Info("End query ", query)
		columns, err := rows.Columns()
		if err != nil {
			glog.V(1).Info("mysql Query return err with :", err)
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

		glog.V(2).Info("start fetch ")
		for rows.Next() {
			// get RawBytes from data
			if err = rows.Scan(scanArgs...); err != nil {
				glog.V(1).Info("mysql rows scan eturn err with :", err)
				return result, err
			}
		}

		glog.V(2).Info("end fetch ")

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
		glog.V(1).Info("mysql Query return error :", err)
	}

	glog.V(2).Info("end getQueryResult ", query)

	return result, err
}

func getStatus(host, username, password string, interval time.Duration, sbm int) {

	var once sync.Once
	db, err := sql.Open("mysql", username+":"+password+"@tcp("+host+")/")
	defer db.Close()
	mysql.SetLogger(LoggerFunc(glog.V(0).Info))

	if err != nil {
		glog.V(1).Info("mysql Open return err with :", err)
	}

	for {
		gLastCheckTime = time.Now()
		glog.V(2).Info("start checking")
		//check if the server is master
		dbMaster := false
		dbSlave := false
		dbRuns := false

		done := make(chan bool, 1)

		go func() {

			if result, err := getQueryResult(db, "show variables like 'read_only';"); err == nil {
				dbRuns = true
				if result["Value"] == "OFF" {
					dbMaster = true
				}
			}

			if result, err := getQueryResult(db, "show slave status;"); err == nil {
				dbRuns = true
				if v, ok := result["Seconds_Behind_Master"]; ok {
					if sbmVal, ok := strconv.Atoi(v); ok == nil && sbmVal <= sbm {
						dbSlave = true
					} else {
						glog.V(1).Info("Seconds_Behind_Master is ", v)
					}
				}
			} else {
				glog.V(1).Info("Query failed with error ", err)
			}

			done <- true
		}()

		timer := time.NewTimer(interval * time.Millisecond)
		select {
		case <-done:
		case <-timer.C:
			dbMaster = false
			dbSlave = false
			dbRuns = false
			glog.V(1).Info("MYSQL TIMEOUT!")
		}
		timer.Stop()

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

		sleepInterval := interval*time.Millisecond - time.Now().Sub(gLastCheckTime)

		once.Do(startService)

		glog.V(2).Info("end checking with sleep ", sleepInterval)
		time.Sleep(sleepInterval)
	}
}

func startService() {
	go func() {
		// your http.Handle calls here
		http.Handle("/checkMaster", http.HandlerFunc(checkMaster))
		http.Handle("/checkSlave", http.HandlerFunc(checkSlave))
		http.Handle("/checkLiveSlave", http.HandlerFunc(checkLiveSlave))

		err := http.ListenAndServe(cfg.Settings.Port, nil)
		if err != nil {
			glog.Fatalf("ListenAndServe: %s", err)
		}
	}()

}

/*type MyLoggerT struct {
}

func (MyLoggerT) Print(v ...interface{}) {
	glog.V(0).Info(v)
}
*/

type LoggerFunc func(v ...interface{})

func (f LoggerFunc) Print(v ...interface{}) {
	f(v)
}

func main() {
	cfg = CFG{
		Settings: SETTINGS{
			Username:      "mha",
			Password:      "",
			Host:          "locahost:3306",
			Interval:      1,
			Sbm:           150,
			Port:          ":3300",
			MaxDelayAllow: 5,
		},
	}

	cfgFile := flag.String("cfg", "./mysql_ms_checker.cfg", "configuration file for mysql master/slaver checher")
	if err := gcfg.ReadFileInto(&cfg, *cfgFile); err != nil {
		glog.V(0).Info("Failed to parse gcfg data: %s", err)
	}
	username := flag.String("u", cfg.Settings.Username, "user name")
	password := flag.String("p", cfg.Settings.Password, "password")
	host := flag.String("h", cfg.Settings.Host, "host")
	interval := flag.Int("i", cfg.Settings.Interval, "interval of the check as second")
	sbm := flag.Int("sbm", cfg.Settings.Sbm, "Second behind master threshold")

	flag.Parse()
	gDelayAllow = time.Duration(cfg.Settings.MaxDelayAllow) * time.Millisecond
	glog.V(1).Info("max delay allow is:", gDelayAllow)

	glog.V(1).Info("mysql_ms_checker start")
	getStatus(*host, *username, *password, time.Duration(*interval), *sbm)
	glog.V(1).Info("mysql_ms_checker end")
}
