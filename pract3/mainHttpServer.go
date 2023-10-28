package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"
)

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
	con, err := net.Dial("tcp", "127.0.0.1:6379")

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
	con, err := net.Dial("tcp", "127.0.0.1:6379")

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
	con, err := net.Dial("tcp", "127.0.0.1:6379")

	if err != nil {
		return errors.New("Database Unavailable")
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
		fmt.Fprintf(w, "Short URL: 127.0.0.1:8080/%s", shortURL)
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
		http.Redirect(w, r, outLink, http.StatusSeeOther)

	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
}
