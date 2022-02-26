package main

import (
	"cassandra_gocql_crud_operations/cassConnection"
	"cassandra_gocql_crud_operations/cassCrudManager"
	"cassandra_gocql_crud_operations/model"
	"fmt"

	"github.com/gocql/gocql"
)

var P1 = model.Profile{
	Key:   "Key1",
	Value: 10,
	Tel:   20,
	Age:   30,
	Name:  "Test1",
}
var P2 = model.Profile{
	Key:   "Key2",
	Value: 101,
	Tel:   202,
	Age:   303,
	Name:  "Test2",
}
var P3 = model.Profile{
	Key:   "Key3",
	Value: 103,
	Tel:   203,
	Age:   307,
	Name:  "Test3",
}

var P4 = model.Profile{
	Key:   "Key4",
	Value: 51,
	Tel:   93,
	Age:   14,
	Name:  "Test4",
}

func InsertAllData(Session *gocql.Session) {
	cassCrudManager.InsertData(P1, "data", Session)
	cassCrudManager.InsertData(P2, "data", Session)
	cassCrudManager.InsertData(P3, "data", Session)
	cassCrudManager.InsertData(P4, "data", Session)
}

func main() {
	Session := cassConnection.ConnectCassandra()
	InsertAllData(Session)

	data, err1 := cassCrudManager.GetByKey("data", "Key3", 103, Session)
	if err1 != nil {
		if err1.Error() == "not found" {
			fmt.Println("veri bulunamadı")
			return
		}
		fmt.Println(err1.Error())
	}
	fmt.Println(*data)

	values, err2 := cassCrudManager.GetAllData("data", Session)
	if err2 != nil {
		if err2.Error() == "not found" {
			fmt.Println("veri bulunamadı")
			return
		}
		fmt.Println("GetAllData Error : ", err2.Error())
		return
	}
	fmt.Println(*values)

	maxValue, err3 := cassCrudManager.GetMaxByValue("data", Session)
	if err3 != nil {
		if err3.Error() == "not found" {
			fmt.Println("veri bulunamadı")
			return
		}
		fmt.Println("GetMaxByValue Error : ", err3.Error())
		return
	}
	fmt.Println("Max Value : ", maxValue)
	fmt.Println("Max Value + 83 : ", maxValue+83)

	P4.Name = "new changed !!!"
	P4.Tel = 999
	upt_err := cassCrudManager.UpdateById("data", "Key4", 51, P4, Session)
	if upt_err != nil {

		fmt.Println("UpdateById Error : ", upt_err.Error())
		return
	}

	values_new, err_new := cassCrudManager.GetAllData("data", Session)
	if err_new != nil {
		if err_new.Error() == "not found" {
			fmt.Println("veri bulunamadı")
			return
		}
		fmt.Println("GetAllData Error : ", err_new.Error())
		return
	}
	fmt.Println(values_new)
	
	err4 := cassCrudManager.DeleteById("data", "Key4", 51, Session)
	if err4 != nil {

		fmt.Println("DeleteById Error : ", err4.Error())
		return
	}

	values_final, err_final := cassCrudManager.GetAllData("data", Session)
	if err_new != nil {
		if err_new.Error() == "not found" {
			fmt.Println("veri bulunamadı")
			return
		}
		fmt.Println("GetAllData Error : ", err_final.Error())
		return
	}
	fmt.Println(*values_final)

}
