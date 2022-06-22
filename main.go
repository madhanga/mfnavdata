package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	db := db()
	defer db.Close()
	start := time.Now()

	schemeCodes, err := getSchemeCodes()
	if err != nil {
		log.Fatal(err)
	}

	schemeCodes = filterSchemeCodesIfAny(schemeCodes)
	err = deleteSchemesFromDbIfAny(db, schemeCodes)
	if err != nil {
		log.Fatalf("error in deleteing the existing entries %v: %v", schemeCodes, err)
	}

	var completedSchemeCount int
	var navEntriesAdded int

	nrOfWorkers := 100
	jobChannel := make(chan string, nrOfWorkers)
	var wg sync.WaitGroup
	wg.Add(nrOfWorkers)

	var mu sync.Mutex

	for i := 0; i < nrOfWorkers; i++ {
		go func() {
			defer wg.Done()
			for schemeCode := range jobChannel {

				navs, err := getHistoricalNAV(schemeCode)
				if err != nil {
					log.Printf("error in getting Historical NAV for scheme: %s: %v", schemeCode, err)
					continue
				}
				isins := schemeCodes[schemeCode]

				for _, isin := range isins {
					val := ""
					for date, value := range navs {
						if val != "" {
							val = val + ","
						}
						val = val + fmt.Sprintf("('%s', '%s', %f, '%s')", isin, schemeCode, value, date)
					}
					query := `insert into nav_history (isin, scheme_code, nav_value, nav_date) values ` + val
					_, err = db.Exec(query)
					if err != nil {
						log.Printf("error in adding to db for isin: %s: %v", isin, err)
						continue
					}
				}

				mu.Lock()
				fmt.Printf("Done for %d of %d schemes\r", completedSchemeCount, len(schemeCodes))
				navEntriesAdded += len(navs) * len(isins)
				completedSchemeCount += 1
				mu.Unlock()
			}
		}()
	}

	go func() {
		for schemeCode := range schemeCodes {
			jobChannel <- schemeCode
		}
		close(jobChannel)
	}()

	wg.Wait()

	elapsed := time.Since(start)
	log.Printf("Filled in %d NAV entries for %d schemes in %v", navEntriesAdded, completedSchemeCount, elapsed)
}

func db() *sql.DB {
	dbUserName := os.Getenv("DATABASE_USERNAME")
	dbPassword := os.Getenv("DATABASE_PASSWORD")
	dbName := os.Getenv("DATABASE_NAME")
	dbHost := os.Getenv("DATABASE_URL")
	log.Println(dbUserName, dbPassword, dbName, dbHost)
	if dbUserName == "" || dbPassword == "" || dbName == "" || dbHost == "" {
		log.Fatal("Database credentials not set")
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s?sslmode=disable&connect_timeout=10", dbUserName, dbPassword, dbHost, dbName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	return db
}

func filterSchemeCodesIfAny(schemeCodes map[string][]string) map[string][]string {
	if len(os.Args) > 1 {
		onlySchemeCodes := os.Args[1:]
		temp := map[string][]string{}
		for _, onlySchemeCode := range onlySchemeCodes {
			temp[onlySchemeCode] = schemeCodes[onlySchemeCode]
		}
		schemeCodes = temp
	}

	return schemeCodes
}

func deleteSchemesFromDbIfAny(db *sql.DB, schemeCodes map[string][]string) error {
	temp := ""
	for scheme := range schemeCodes {
		if temp != "" {
			temp = temp + ","
		}
		temp = temp + fmt.Sprintf("'%s'", scheme)
	}
	query := "delete from nav_history where scheme_code in (" + temp + ")"
	_, err := db.Exec(query)
	return err
}

func getHistoricalNAV(schemeCode string) (map[string]float64, error) {
	url := fmt.Sprintf("http://angelbrokingapi.cmots.com/api/SchemeNAVHistorical/%s/Y/20", schemeCode)

	data, err := getCMOTS(url)
	if err != nil {
		return nil, err
	}

	if data["data"] == nil {
		return nil, fmt.Errorf("%s does not have historical nav", schemeCode)
	}

	navs := map[string]float64{}
	for _, tempdata := range data["data"].([]interface{}) {
		navi := tempdata.(map[string]any)
		date := navi["NAVDATE"].(string)
		value := navi["NAVRS"].(float64)
		navs[date] = value
	}

	return navs, nil
}

func getSchemeCodes() (map[string][]string, error) {
	url := "http://angelbrokingapi.cmots.com/api/SchemeMaster"
	data, err := getCMOTS(url)
	if err != nil {
		return nil, err
	}

	result := map[string][]string{}
	schemesI := data["data"].([]any)
	for _, schemeI := range schemesI {
		scheme := schemeI.(map[string]any)
		schemeCode := fmt.Sprintf("%.1f", scheme["mf_schcode"].(float64))
		var isins []string
		if isin := scheme["isin"]; isin != nil {
			isins = append(isins, isin.(string))
		}
		if reIsin := scheme["isin_Reinvestment"]; reIsin != nil {
			isins = append(isins, reIsin.(string))
		}
		result[schemeCode] = isins
	}
	return result, nil
}

func getCMOTS(url string) (map[string]any, error) {
	bearerToken := "Bearer FkLpiVedKizrjkML771_wJ-vEKMPKVKrNzZHSAe2yipPt8jDyssu-l8GOVh1UrZs8dI05kNT_Jyjf7-Hi9Q7QDLaod844f_wb31hxDtBpWcf3DekV1AsIGifKUJJePgRw8BzC-xg-7Vb0ylK8YbgY72JYYPNFp-Vqs6xqA0W0wsGo9ouu2CXf5MPHW7qLrMdpQjLGp6EZJIKVGNloAvjfnhKoajHqVoUiAUbpZJfM-o6epe-edbRRN5WxN2FuIVPoEA9v-Uh_LIK5k5p9wm5xx5cww72r1uc3SD3TSo2nosdhreIFCcyMxLGNzG-In0f"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", bearerToken)

	var data map[string]any
	var errValue error
	for i := 0; i < 3; i++ {
		client := http.Client{}
		res, err := client.Do(req)
		if err != nil {
			errValue = err
			time.Sleep(1 * time.Second)
			continue
		}
		defer res.Body.Close()

		err = json.NewDecoder(res.Body).Decode(&data)
		if err != nil {
			return nil, err
		}
		break
	}

	if data == nil {
		return nil, errValue
	}

	return data, nil
}
