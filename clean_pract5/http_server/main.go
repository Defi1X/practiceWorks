package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"
)

var DATABASE_ADDRESS = "database_server:6379"
var STATS_ADDRESS = "http://stats_server:6565"

type connectionReport struct {
	ShortUrl string `json:"shortURL"`
	OutLink  string `json:"outLink"`
	Host     string `json:"originHost"`
}

func sendStats(a string, b string, c string) {
	some := connectionReport{
		ShortUrl: a,
		OutLink:  strings.Trim(b, "\u0000"),
		Host:     c,
	}

	jsonPost, err := json.Marshal(some)

	fmt.Println("gonna send:", string(jsonPost), err)

	data := []byte(jsonPost)
	r := bytes.NewReader(data)

	_, err = http.Post(STATS_ADDRESS, "application/json", r)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func generateShortLink(link string) (string, error) {
	alphabet := "QWERTYUIOPASDFGHJKLZXCVBNM"
	alphabet = alphabet + strings.ToLower(alphabet) + "1234567890"
	shortLinkChars := ""

	for {
		shortLinkChars = ""
		for i := 0; i < 9; i++ {
			shortLinkChars += string(alphabet[rand.Intn(len(alphabet))])
		}

		_, err := baseFindLink(shortLinkChars)

		fmt.Println("Err:", err.Error())
		if err.Error() == "Link does not exist" {
			break
		}

	}
	fmt.Println("exiting generate with", shortLinkChars)
	return shortLinkChars, nil
}

func baseFindLink(shortLink string) (string, error) {
	fmt.Println("baseFindLink(", shortLink, ")")
	con, err := net.Dial("tcp", DATABASE_ADDRESS)

	if err != nil {
		return "", errors.New("Database Unreachable")
	}

	defer con.Close()

	msg := "--file siteDB --query \"HGET linksHashtable " + shortLink + "\""

	_, err = con.Write([]byte(msg))

	if err != nil {
		return "", err
	}

	reply := make([]byte, 512)

	_, err = con.Read(reply)

	if err != nil {
		return "", err
	}

	cleanReply := strings.TrimSpace(string(reply))
	cleanReply = strings.ReplaceAll(cleanReply, "\n", "")
	// fmt.Println(":::::", []byte(cleanReply), ":::::")
	if cleanReply[0:13] == "Key not found" {
		return "", errors.New("Link does not exist")
	} else {
		return cleanReply, nil
	}
}

func baseAddLink(shortLink string, longLink string) error {
	fmt.Println("baseAddLink(", shortLink, ",", longLink, ")")
	con, err := net.Dial("tcp", DATABASE_ADDRESS)

	if err != nil {
		return errors.New("Database Unreachable")
	}

	defer con.Close()

	msg := "--file siteDB --query \"HSET linksHashtable " + shortLink + " " + longLink + "\""

	_, err = con.Write([]byte(msg))

	if err != nil {
		return err
	}

	return nil
}

func initializeBase() error {
	con, err := net.Dial("tcp", DATABASE_ADDRESS)

	if err != nil {
		return err
	}

	defer con.Close()

	msg := "--file siteDB --query \"HSET linksHashtable _test initializationkey\""

	_, err = con.Write([]byte(msg))

	if err != nil {
		return err
	}

	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano())
	err := initializeBase()

	if err != nil {
		fmt.Println(err)
		return
	} else {
		fmt.Println("DB accessable!")
	}

	http.HandleFunc("/", connectionHandler)

	log.Fatal(http.ListenAndServe(":8080", nil))
}

func connectionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		longUrl := r.FormValue("url")
		if longUrl == "" {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		shortURL, _ := generateShortLink(longUrl)

		err := baseAddLink(shortURL, longUrl)

		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		// Отправка короткой ссылки в ответе
		fmt.Fprintf(w, "Short URL: 127.0.0.1:80/%s", shortURL)
	} else if r.Method == http.MethodGet {
		shortUrl := r.URL.Path[1:]

		result, err := baseFindLink(shortUrl)

		fmt.Println("result <<<", result, ">>> error: <<<", err, ">>>")

		if err != nil {
			if err.Error() == "Link does not exist" {
				http.NotFound(w, r)
				return
			} else {
				http.Error(w, "Internal server error"+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		outLink := ""

		if result[0:4] != "http" {
			fmt.Println(result[0:4])
			outLink = "http://" + result
		} else {
			outLink = result
		}

		outLink = strings.ReplaceAll(outLink, "\n", "")
		fmt.Println("outlink <", outLink, ">")

		host, _, _ := net.SplitHostPort(r.RemoteAddr)

		sendStats(shortUrl, outLink, host)

		http.Redirect(w, r, outLink, http.StatusSeeOther)

	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
}
