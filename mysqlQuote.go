package main

import (
  "fmt"
  "os"
  "sync"
  "path/filepath"
  "gopkg.in/ini.v1"
  "database/sql"
  _ "github.com/go-sql-driver/mysql"
  "golang.org/x/text/language"
  "golang.org/x/text/message"
)

type MYSQL struct {
  host     string
  port     int
  user     string
  pswd     string
  dbase    string
}

type COMMON struct {
  earliest string
  latest   string
}

type INI struct {
  mysql1  MYSQL
  mysql2  MYSQL
  common  COMMON
}

type ROW struct {
  date  string  `json:"date"`
  count int     `json:"count"`
}


type ResultItem struct {
  Items []ROW
}

func readINI() INI {
  // --- find ini file ---
  file, _ := os.Readlink("/proc/self/exe")

  // --- read ini file ---
  cfg, err := ini.Load(filepath.Join(filepath.Dir(file), "mysqlQuote.ini"))
  if err != nil {
    panic(err.Error())
  }

  port1, _ := cfg.Section("mysql1").Key("port").Int() 
  port2, _ := cfg.Section("mysql2").Key("port").Int() 
  ini := INI {
                mysql1: MYSQL {
                  cfg.Section("mysql1").Key("host").String(),
                  port1,
                  cfg.Section("mysql1").Key("user").String(),
                  cfg.Section("mysql1").Key("pswd").String(),
                  cfg.Section("mysql1").Key("dbase").String(),
                },
                mysql2: MYSQL {
                  cfg.Section("mysql1").Key("host").String(),
                  port2,
                  cfg.Section("mysql1").Key("user").String(),
                  cfg.Section("mysql1").Key("pswd").String(),
                  cfg.Section("mysql1").Key("dbase").String(),
                },
                common: COMMON {
                  cfg.Section("common").Key("earliest").String(),
                  cfg.Section("common").Key("latest").String(),
                },
             }
  return ini
}

func (res *ResultItem) AddItem(item ROW) []ROW {
  res.Items = append(res.Items, item)
  return res.Items
}

func main() {
  ini := readINI()
  
  // --- open db connections ---

  // -- 1st --
  dsn1 := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
                        ini.mysql1.user,
                        ini.mysql1.pswd,
                        ini.mysql1.host,
                        ini.mysql1.port,
                        ini.mysql1.dbase)
  db1, err := sql.Open("mysql", dsn1)
  if err != nil {
    panic(err.Error())
  }
  defer db1.Close()

  // -- 2nd --
  dsn2 := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
                        ini.mysql2.user,
                        ini.mysql2.pswd,
                        ini.mysql2.host,
                        ini.mysql2.port,
                        ini.mysql2.dbase)
  db2, err := sql.Open("mysql", dsn2)
  if err != nil {
    panic(err.Error())
  }
  defer db2.Close()

  // --- build query ---
  sql := `SELECT
            CONCAT(month(UsageDate), "/", day(UsageDate)) AS date,
            COUNT(*) AS count
          FROM
            tblQuoteUsageSplunk
          WHERE
            UsageDate BETWEEN '` + ini.common.earliest + `' AND '` + ini.common.latest + `'
          GROUP BY
            date`

  // --- execute queries ---

  var row1 ROW
  var row2 ROW
  result1 := ResultItem{}
  result2 := ResultItem{}

  var wg sync.WaitGroup
  wg.Add(2)

  go func() {
    defer wg.Done()
    results1, err := db1.Query(sql)
    if err != nil {
      panic(err.Error())
    }
    defer results1.Close()
    
    for results1.Next() {
      err = results1.Scan(&row1.date, &row1.count)
      if err != nil {
        panic(err.Error())
      }
      result1.AddItem(row1)
    }
  }()

  
  go func() {
    defer wg.Done()
    results2, er := db2.Query(sql)
    if er != nil {
      panic(er.Error())
    }
    defer results2.Close()
    
    for results2.Next() {
      err = results2.Scan(&row2.date, &row2.count)
      if err != nil {
        panic(err.Error())
      }
      result2.AddItem(row2)
    }
  }()
  wg.Wait()

  len1 := len(result1.Items)

  p := message.NewPrinter(language.English)
  fmt.Printf("%s\t%10s\t%10s\n", "Day", "Prod", "AWS")
  
  for i := 0; i < len1; i++ {    
    p.Printf("%s\t%10d\t%10d\n",
                  result1.Items[i].date,
                  result1.Items[i].count,
                  result2.Items[i].count)
  }
}
