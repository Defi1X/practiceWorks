package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

type connectionReport struct {
	ShortUrl string `json:"shortURL"`
	OutLink  string `json:"outLink"`
	Host     string `json:"originHost"`
}

type JSONEntry struct {
	ID       int    `json:"id"`
	PID      int    `json:"pid"`
	URL      string `json:"url"`
	ShortURL string `json:"shortURL"`
	SourceIP string `json:"sourceIP"`
	Time     string `json:"time"`
	Count    int    `json:"count"`
}

type Payload struct {
	Dimensions []string `json:"Dimensions"`
}

func newRedirectHandler(w http.ResponseWriter, r *http.Request) {
	var reportData connectionReport
	err := json.NewDecoder(r.Body).Decode(&reportData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	statConnections(reportData.OutLink, reportData.ShortUrl, reportData.Host)

	fmt.Printf("Received report: %+v\n", reportData)
	fmt.Println(reportData.ShortUrl)
	fmt.Println(reportData.OutLink)
	fmt.Println(reportData.Host)

	return
}
func reportHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload Payload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	fmt.Println("Received dimensions:", payload.Dimensions)

	response, err := os.ReadFile("connections.json")
	if err != nil {
		fmt.Println("Error reading from file:", err)
		return
	}

	var JsonFile []JSONEntry

	if len(response) == 0 {
		return
	}

	err = json.Unmarshal(response, &JsonFile)
	if err != nil {
		return
	}

	jsonData := make_report(payload.Dimensions, JsonFile)

	err = write_json_file(jsonData, "report.json")

	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}

	data, err := ioutil.ReadFile("report.json")

	if err != nil {
		log.Fatal(err)
		w.WriteHeader(http.StatusInternalServerError)
	}

	reportContent := string(data)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(reportContent))
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("Stats server up at 127.0.0.1:6565")

	http.HandleFunc("/", newRedirectHandler)
	http.HandleFunc("/report", reportHandler)

	log.Fatal(http.ListenAndServe(":6565", nil))
}

func statConnections(url, shortURL, ip string) {
	parent_conn := JSONEntry{
		URL:      url,
		ShortURL: shortURL,
		Count:    1,
	}

	new_conn := JSONEntry{
		SourceIP: ip,
		Time:     time.Now().Format("2999-01-02 00:00"),
		Count:    1,
	}

	conns, err := read_base()
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	if conns == nil {
		conns = []JSONEntry{}
	}

	parent_conn.ID = gen_unpid(conns)
	if unique_par(conns, parent_conn.URL) == true {
		conns = append(conns, parent_conn)
	} else {
		par_count(conns, parent_conn.URL)
	}

	new_conn.ID = gen_unpid(conns)
	new_conn.PID = gen_pid(conns, url)
	conns = append(conns, new_conn)

	err = write_base(conns)
	if err != nil {
		fmt.Println("Error reading from file:", err)
		return
	}

}

func read_base() ([]JSONEntry, error) {
	var conn []JSONEntry

	file, err := os.ReadFile("connections.json")
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	if len(file) == 0 {
		return nil, nil
	}

	err = json.Unmarshal(file, &conn)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func write_base(workers []JSONEntry) error {
	jsonData, err := json.MarshalIndent(workers, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile("connections.json", jsonData, 0644)
	if err != nil {
		return err
	}

	return nil
}

func unique_par(conn []JSONEntry, url string) bool {
	for _, connect := range conn {
		if connect.URL == url {
			return false
		}
	}
	return true
}

func par_count(conn []JSONEntry, url string) {
	for index := range conn {
		if conn[index].URL == url {
			conn[index].Count++
			return
		}
	}
}

func gen_pid(conn []JSONEntry, url string) int {
	PID := 0
	for _, connect := range conn {
		if connect.URL == url {
			PID = connect.ID
		}
	}
	return PID
}
func gen_unpid(conn []JSONEntry) int {
	maxID := 0
	for _, connect := range conn {
		if connect.ID > maxID {
			maxID = connect.ID
		}
	}
	return maxID + 1
}

func write_json_file(data interface{}, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(data); err != nil {
		return err
	}

	return nil
}

func search_url_id(id int, conn []JSONEntry) string {
	for _, conn := range conn {
		if conn.ID == id {
			return conn.URL
		}
	}
	return ""
}

func search_short_url_id(id int, conn []JSONEntry) string {
	for _, conn := range conn {
		if conn.ID == id {
			return conn.ShortURL
		}
	}
	return ""
}
func make_report(detalizationList []string, connections_base []JSONEntry) map[string]interface{} {
	report := make(map[string]interface{})

	for _, conn := range connections_base {
		if conn.PID == 0 {
			continue
		}

		ip := conn.SourceIP
		Time := conn.Time[11:]
		shortURL := search_short_url_id(conn.PID, connections_base)
		url := search_url_id(conn.PID, connections_base) + " (" + shortURL + ")"

		current_level := report
		for _, level := range detalizationList {
			if level == "SourceIP" {
				_, ipfound := current_level[ip]
				if !ipfound {
					current_level[ip] = make(map[string]interface{})
					_, found := current_level["Count"]
					if !found {
						current_level["Count"] = 0
					}
				}
				current_level = current_level[ip].(map[string]interface{})
			} else if level == "TimeInterval" {
				_, timefound := current_level[Time]
				if !timefound {
					current_level[Time] = make(map[string]interface{})
					_, found := current_level["Count"]
					if !found {
						current_level["Count"] = 0
					}
				}
				current_level = current_level[Time].(map[string]interface{})
			} else if level == "URL" {
				_, urlfound := current_level[url]
				if !urlfound {
					current_level[url] = make(map[string]interface{})
					_, found := current_level["Count"]
					if !found {
						current_level["Count"] = 0
					}
				}
				current_level = current_level[url].(map[string]interface{})
			}

			_, found := current_level["Count"]
			if !found {
				current_level["Count"] = 0
			}
			current_level["Count"] = current_level["Count"].(int) + 1
		}
	}

	delete(report, "Count")

	return report
}
