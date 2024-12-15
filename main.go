// main.go
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

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

func NumericFloat64toFloat64(val pgtype.Numeric) (float64, error) {
	if val.NaN {
		return math.NaN(), nil
	}

	// var intValue int64

	intValue, _ := val.Int.Float64()

	// fmt.Println(intValue * math.Pow10(int(val.Exp)))

	// if err := val.Int(&intValue); err != nil {
	// 	return 0, err
	// }

	exp := val.Exp

	return intValue * math.Pow10(int(exp)), nil
	// return 0.0, nil
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

func Query(queryString string, c int, wg *sync.WaitGroup, envFile map[string]string) {
	defer wg.Done()
	ctx := context.Background()
	conn, err := ConnectPostgres(ctx, envFile)
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, queryString)
	if err != nil {
		fmt.Println("Fail to query,", queryString, "error:", err.Error())
	}

	// Create a CSV file
	file, err := os.Create(fmt.Sprintf("output-%d.csv", c))
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v\n", err)
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header
	columns := make([]string, len(rows.FieldDescriptions()))
	for i, field := range rows.FieldDescriptions() {
		columns[i] = string(field.Name)
	}
	if err := writer.Write(columns); err != nil {
		log.Fatalf("Failed to write header to CSV: %v\n", err)
	}

	// Write the rows
	for rows.Next() {
		record := make([]string, len(columns))

		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{}) // Use a pointer to interface{}
		}

		if err := rows.Scan(values...); err != nil {
			log.Fatalf("Scan failed: %v\n", err)
		}

		for i, field := range rows.FieldDescriptions() {
			oid := field.DataTypeOID

			switch oid {
			case pgtype.NumericOID:
				// var num pgtype.Numeric
				// (*values[i]).(float64)
				t := *(values[i].(*interface{}))
				k := t.(pgtype.Numeric)
				// record[i] = fmt.Sprintf("%v", k)
				valf, _ := NumericFloat64toFloat64(k)
				record[i] = strconv.FormatFloat(valf, 'f', 6, 64)
			case pgtype.Float8OID:
				rawFloat := *(values[i].(*interface{}))
				record[i] = fmt.Sprintf(`%s`, strconv.FormatFloat(rawFloat.(float64), 'G', 15, 64))
			case pgtype.VarcharOID:
				// fmt.Printf(`"%s`, *(values[i].(*interface{})))
				record[i] = fmt.Sprintf(`%s`, *(values[i].(*interface{})))
			case pgtype.TimestampOID:
				rawts := *(values[i].(*interface{}))
				if rawts != nil {
					record[i] = fmt.Sprintf(`%v`, rawts.(time.Time).Format("2006-01-02 15:04:00"))
				}
			case pgtype.Int4OID:
				record[i] = fmt.Sprintf("%d", *(values[i].(*interface{})))
			default:
				record[i] = fmt.Sprintf(`"%#V`, *(values[i].(*interface{})))
			}
		}

		// Write the record to the CSV
		if err := writer.Write(record); err != nil {
			log.Fatalf("Failed to write record to CSV: %v\n", err)
		}
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Row iteration failed: %v\n", err)
	}

	fmt.Println("Data successfully written to output.csv")

}

func readFile(fileName string, wg *sync.WaitGroup, envFile map[string]string) {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Fail to read sql file.", err.Error())
		os.Exit(READSQLFAIL)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	c := 0
	for scanner.Scan() {
		wg.Add(1)
		tx := scanner.Text()
		// go fmt.Println(tx)
		go Query(tx, c, wg, envFile)
		c++
	}

	if err = scanner.Err(); err != nil {
		fmt.Println("Fail when read sql file.", err.Error())
		os.Exit(READSQLFAIL)
	}
}

func main() {
	var conn *pgx.Conn
	var err error
	var wg sync.WaitGroup

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
			readFile(args[0], &wg, envFile)
		}
	}

	wg.Wait()
}
