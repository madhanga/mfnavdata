package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:: mfnavdata <CMOTS auth token>")
		return
	}
	token := os.Args[1]
	bearerToken := "Bearer " + token
	schemeMasterURL := "http://angelbrokingapi.cmots.com/api/SchemeMaster"
	historicalNAVURL := "http://angelbrokingapi.cmots.com/api/SchemeNAVHistorical/%s/Y/20"

	connStr := "postgresql://postgres:zenith@localhost/mfrant?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	schemeCodes, err := getSchemeCodes(bearerToken, schemeMasterURL)
	if err != nil {
		panic(err)
	}

	type resultData struct {
		SchemeCode string
		Err        error
	}

	nrOfWorkers := 25
	jobChannel := make(chan string, nrOfWorkers)
	resultChannel := make(chan resultData, nrOfWorkers)

	for i := 0; i < nrOfWorkers; i++ {
		go func() {
			for schemeCode := range jobChannel {
				historicalNAVURL := fmt.Sprintf(historicalNAVURL, schemeCode)
				historicalNAV, err := getHistoricalNAV(bearerToken, historicalNAVURL)
				if err != nil {
					resultChannel <- resultData{SchemeCode: schemeCode, Err: err}
					continue
				}

				b := new(strings.Builder)
				err = json.NewEncoder(b).Encode(historicalNAV)
				if err != nil {
					resultChannel <- resultData{SchemeCode: schemeCode, Err: err}
					continue
				}

				query := `insert into nav (isin, navdata) values ($1, $2)`
				_, err = db.Exec(query, schemeCode, b.String())
				if err != nil {
					resultChannel <- resultData{SchemeCode: schemeCode, Err: err}
					continue
				}

				resultChannel <- resultData{SchemeCode: schemeCode, Err: nil}
			}
		}()
	}

	go func() {
		for _, schemeCode := range schemeCodes {
			jobChannel <- schemeCode
		}
		close(jobChannel)
	}()

	for range schemeCodes {
		result := <-resultChannel
		temp := "Processed " + result.SchemeCode
		if result.Err != nil {
			temp += " with error: " + result.Err.Error()
		}
		fmt.Println(temp)
	}
	close(resultChannel)
}

//func getHistoricalNAV(bearerToken string, url string) ([]NavData, error) {
func getHistoricalNAV(bearerToken string, url string) ([]map[string]any, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", bearerToken)

	client := http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var data map[string]any
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return nil, err
	}

	var navs []map[string]any
	for _, tempdata := range data["data"].([]interface{}) {
		navi := tempdata.(map[string]any)
		navdata := map[string]any{}
		navdata["date"] = navi["NAVDATE"].(string)
		navdata["nav"] = navi["NAVRS"].(float64)
		navs = append(navs, navdata)
	}

	return navs, nil
}

func getSchemeCodes(bearerToken string, schemeMasterURL string) ([]string, error) {
	req, err := http.NewRequest("GET", schemeMasterURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", bearerToken)

	client := http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var data map[string]any
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return nil, err
	}

	schemesI := data["data"].([]any)
	var schemeCodes []string
	for _, schemeI := range schemesI {
		scheme := schemeI.(map[string]any)
		schemeCode := fmt.Sprintf("%.1f", scheme["mf_schcode"].(float64))
		schemeCodes = append(schemeCodes, schemeCode)
	}

	return schemeCodes, nil

}
