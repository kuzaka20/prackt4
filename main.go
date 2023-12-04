package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
)

var mu sync.Mutex

func shortenHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.ParseForm()
	originalURL := r.Form.Get("url")

	if originalURL == "" {
		http.Error(w, "URL is required", http.StatusBadRequest)
		return
	}

	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Ошибка при подключении к серверу:", err)
		os.Exit(1)
	}
	defer conn.Close()
	mu.Lock()
	defer mu.Unlock()

	shortURL := generateShortURL()
	_, err = conn.Write([]byte("HSET " + originalURL + " " + shortURL + "\n"))
	if err != nil {
		fmt.Println("Ошибка при отправке команды на сервер:", err)
		return
	}
	err_conn, er := bufio.NewReader(conn).ReadString('\n')
	if er != nil {
		fmt.Println("Ошибка при чтении ответа от сервера:", er)
		return
	}
	shortURL = err_conn
	if err != nil {
		fmt.Println(err)
	}

	fmt.Fprintf(w, "Shortened URL: http://localhost:8090/%s", shortURL)
}

func redirectHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Ошибка при подключении к серверу:", err)
		os.Exit(1)
	}
	defer conn.Close()
	mu.Lock()

	shortURL := strings.TrimPrefix(r.URL.Path, "/")
	_, err = conn.Write([]byte("HGET " + shortURL + "\n"))
	if err != nil {
		fmt.Println("Ошибка при отправке команды на сервер:", err)
		return
	}
	originalURL, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("Ошибка при чтении ответа от сервера:", err)
		return
	}
	if originalURL != "Элемент не найден" {
		http.Redirect(w, r, originalURL, http.StatusFound)

		connStat, errStat := net.Dial("tcp", "localhost:5252")
		if errStat != nil {
			fmt.Println("Ошибка при подключении к серверу:", errStat)
			os.Exit(1)
		}
		defer connStat.Close()

		_, err = connStat.Write([]byte("1 " + originalURL[:len(originalURL)-1] + " " + shortURL + " " + GetClientIP(r) + "\n"))
		if err != nil {
			fmt.Println("Ошибка при отправке команды на сервер:", err)
			return
		}
	} else {
		http.NotFound(w, r)
	}
	mu.Unlock()
}

func generateShortURL() string {
	numb := rand.Intn(6)
	if numb <= 1 {
		numb += 2
	}
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, numb)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}
func main() {
	http.HandleFunc("/shorten", shortenHandler)
	http.HandleFunc("/", redirectHandler)
	http.HandleFunc("/report", reportFunc)
	http.ListenAndServe("localhost:8090", nil)
}

func reportFunc(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	err := request.ParseForm()
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	str := request.Form["strings"]
	resultString := ""
	for _, strs := range str {
		resultString += strs + " "
	}
	connStat, errStat := net.Dial("tcp", "localhost:5252")
	if errStat != nil {
		fmt.Println("Ошибка при подключении к серверу:", errStat)
		os.Exit(1)
	}
	defer connStat.Close()

	_, err = connStat.Write([]byte("2 " + resultString + "\n"))
	if err != nil {
		fmt.Println("Ошибка при отправке команды на сервер:", err)
		return
	}
}
func GetClientIP(r *http.Request) string {
	// Получаем IP-адрес клиента из заголовка X-Real-IP или X-Forwarded-For
	ip := r.Header.Get("X-Real-IP")
	if ip == "" {
		ip = r.Header.Get("X-Forwarded-For")
	}
	// Если заголовки не содержат информацию о IP, используем RemoteAddr
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	}
	return ip
}
