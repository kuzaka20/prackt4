package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

func FuncHash(key string) int { //Преобразование значения в хеш
	hash := 0
	for i := 0; i < len(key); i++ {
		hash += int(key[i])
	}
	return hash % 256

}
func (hashMap *HashTable) insert(key string, value string) error { //Добавление пары ключ значение в Хеш-таблицу
	newKeyValue := &KeyValue{key, value}
	index := FuncHash(key)
	if hashMap.table[index] == nil {
		hashMap.table[index] = newKeyValue
		return nil
	} else {
		if hashMap.table[index].key == key {
			return errors.New("Такой ключ уже сушествует")
		} else {
			for i := index; i < 256; i++ {
				if hashMap.table[i] == nil {
					hashMap.table[i] = newKeyValue
					return nil
				}
			}
		}
	}
	return errors.New("Неудолось добавить элемент")
}
func (hashMap *HashTable) remuve(key string) error { //Удаление пары ключ значение из Хеш-таблицы
	index := FuncHash(key)
	if hashMap.table[index] == nil {
		return errors.New("Элемент не найден")
	} else if hashMap.table[index].key == key {
		hashMap.table[index] = nil
		return nil
	} else {
		for i := index; i < 256; i++ {
			if hashMap.table[i].key == key {
				hashMap.table[i] = nil
				return nil
			}
		}
	}
	return errors.New("Неудалось удалить элемент")
}
func (hashMap *HashTable) hashGet(value string) (string, error) { //Поиск изначения по ключу в Хеш-таблице
	for i := 0; i < 256; i++ {
		if hashMap.table[i] != nil {
			if hashMap.table[i].value == value {
				return hashMap.table[i].key, nil
			}
		}
	}
	return "", errors.New("Элемент не найден")
}
func (hashMap *HashTable) HashGet(key string) (string, error) { //Поиск изначения по ключу в Хеш-таблице
	index := FuncHash(key)
	if hashMap.table[index] == nil {
		return "", errors.New("Элемент не найден")
	} else if hashMap.table[index].key == key {
		return hashMap.table[index].value, nil
	} else {
		for i := index; i < 256; i++ {
			if hashMap.table[i].key == key {
				return hashMap.table[index].value, nil
			}
		}
	}
	return "", errors.New("Элемент не найден")
}
func (hash *HashTable) readHashFile(filename string) { //Запись Хеш-таблицы из файла
	content, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			file, createErr := os.Create(filename)
			if createErr != nil {
				panic(createErr)
			}
			file.Close()
			return
		}
		panic(err)
	}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		parts := strings.Split(line, " ")
		if len(parts) >= 2 {
			key := parts[0]
			value := strings.Join(parts[1:], " ")
			err := hash.insert(key, value)
			if err != nil {
				panic(err)
			}
		}
	}
}
func (hash *HashTable) writesHashFile(filename string) { //Запись Хеш-таблицы в файл
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for i := 0; i < 256; i++ {
		if hash.table[i] != nil {
			_, err = file.WriteString(hash.table[i].key + " " + hash.table[i].value + "\n")
			if err != nil {
				panic(err)
			}
			er := hash.remuve(hash.table[i].key)
			if er != nil {
				panic(er)
			}
		}
	}
	return
}

type KeyValue struct {
	key   string
	value string
}
type HashTable struct {
	table [256]*KeyValue
}

func main() {
	fmt.Println("Сервер создан")

	ln, err := net.Listen("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Ошибка при запуске сервера:", err)
		return
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Ошибка при принятии соединения:", err)
			continue
		}

		go handleConnectionSubd(conn)
	}
}
func handleConnectionSubd(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		input := scanner.Text()
		args := strings.Fields(input)
		actions := args[0]
		var key string
		var value string
		if len(args) == 1 {
			key = ""
			value = ""
		} else if len(args) == 2 {
			key = args[1]
			value = ""
		} else if len(args) == 3 {
			key = args[1]
			value = args[2]
		}
		var mut sync.Mutex
		hashTable := &HashTable{}
		if actions == "HSET" {
			mut.Lock()
			hashTable.readHashFile("Url.txt")
			er := hashTable.insert(key, value)
			if er != nil {
				hash, erro := hashTable.HashGet(key)
				if erro != nil {

				}
				_, err := conn.Write([]byte(hash + "\n"))
				if err != nil {
					fmt.Println("Ошибка при отправке команды на сервер:", err)
					return
				}
			} else {
				_, err := conn.Write([]byte(value + "\n"))
				if err != nil {
					fmt.Println("Ошибка при отправке команды на сервер:", err)
					return
				}
			}
			hashTable.writesHashFile("Url.txt")
			mut.Unlock()
		} else if actions == "HGET" {
			mut.Lock()
			hashTable.readHashFile("Url.txt")
			remove, er := hashTable.hashGet(key)
			if er == nil {
				_, err := conn.Write([]byte(remove + "\n"))
				if err != nil {
					fmt.Println("Ошибка при отправке команды на сервер:", err)
					return
				}
			} else {
				_, err := conn.Write([]byte(er.Error() + "\n"))
				if err != nil {
					fmt.Println("Ошибка при отправке команды на сервер:", err)
					return
				}
			}
			hashTable.writesHashFile("Url.txt")
			mut.Unlock()
		} else if actions == "REPORT" {
			res, err := os.ReadFile("connection.json")
			if err != nil {
				fmt.Println("Ошибка открытия файла:", err)
				return
			}
			_, err = conn.Write(res)
			if err != nil {
				fmt.Println("Ошибка при отправке команды на сервер:", err)
				return
			}
		}
	}
}
