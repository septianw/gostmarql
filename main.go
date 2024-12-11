// main.go
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

const (
	SUCCESS     = iota // 0
	READENVFAIL = iota // 1
	READSQLFAIL = iota // 2
	DBCONNFAIL  = iota // 3
)

func checkEnv(envFile map[string]string) int {

	if strings.Compare(envFile["DBURL"], "") == 0 {
		fmt.Println("DBURL key not found in Environment file.")

		if strings.Compare(envFile["DBHOST"], "") == 0 {
			fmt.Println("DBHOST key not found in Environment file.")
			os.Exit(READENVFAIL)
		}

		if strings.Compare(envFile["DBPORT"], "") == 0 {
			fmt.Println("DBPORT key not found in Environment file.")
			os.Exit(READENVFAIL)
		}

		if strings.Compare(envFile["DBUSER"], "") == 0 {
			fmt.Println("DBUSER key not found in Environment file.")
			os.Exit(READENVFAIL)
		}

		if strings.Compare(envFile["DBPASSWORD"], "") == 0 {
			fmt.Println("DBPASSWORD key not found in Environment file.")
			os.Exit(READENVFAIL)
		}

		if strings.Compare(envFile["DBSCHEMA"], "") == 0 {
			fmt.Println("DBSCHEMA key not found in Environment file.")
			os.Exit(READENVFAIL)
		}
	}

	return SUCCESS
}

func ConnectPostgres(ctx context.Context, envFile map[string]string) (*pgx.Conn, error) {
	var dburl string
	if strings.Compare(envFile["DBURL"], "") == 0 {
		// build query url here
		dburl = fmt.Sprintf("postgres://%s:%s@%s:%d/%s", envFile["DBUSER"], envFile["DBPASSWORD"], envFile["DBHOST"], envFile["DBPORT"], envFile["DBSCHEMA"])
	} else {
		dburl = envFile["DBURL"]
	}

	return pgx.Connect(ctx, dburl)
}

func Query(queryString string, envFile map[string]string) {
	ctx := context.Background()
	conn, err := ConnectPostgres(ctx, envFile)
	defer conn.Close(ctx)

	_, err := conn.Query(ctx, queryString)
	if err != nil {
		fmt.Println("Fail to query,", queryString, "error:", err.Error())
	}
}

func readFile(fileName string, envFile map[string]string) {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Fail to read sql file.", err.Error())
		os.Exit(READSQLFAIL)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		tx := scanner.Text()
		go Query(tx, envFile)
	}

	if err = scanner.Err(); err != nil {
		fmt.Println("Fail when read sql file.", err.Error())
		os.Exit(READSQLFAIL)
	}
}

func main() {
	var conn *pgx.Conn
	var err error
	envFile, err := godotenv.Read(".env")
	if err != nil {
		fmt.Println("Fail to read Environment file", err.Error())
		os.Exit(1)
	}

	flag.String("test", "file", "list file ")
	flag.Parse()

	args := os.Args[1:]

	status := checkEnv(envFile)
	if status == SUCCESS {
		// fmt.Printf("%#v", envFile)
		ctx := context.Background()
		conn, err = ConnectPostgres(ctx, envFile)
		if err != nil {
			fmt.Println("Fail to connect to database server.", err.Error())
			os.Exit(DBCONNFAIL)
		}
		conn.Close(ctx)

		if len(args) == 0 {
			flag.Usage()
		} else {
			readFile(args[0], envFile)
		}
	}

}
